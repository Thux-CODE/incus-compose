package enricher

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestEighthLeftoverNotFoundIsLost: web is held and served. Its read of
// session 2 lands while the door has seen 3, and finds it gone (404). The
// read is a leftover (8d8cfce): the state keeps web, no delete is announced,
// no retry is left. Session 3's run then breaks on the instance listing of
// its project, which a cold store or an earlier run had listed, and ends by
// grace. The round is warm and web, gone from Incus, is still answered: the
// only news of its delete was that 404. Before 8d8cfce the 404 announced the
// delete, stamped with the newest session.
func TestEighthLeftoverNotFoundIsLost(t *testing.T) {
	p := New(slog.Default(), InboxSize(1))
	require.NoError(t, p.reads.start(1))
	t.Cleanup(p.reads.stop)

	p.reads.readProject = func(_ context.Context, name string) (*api.Project, string, error) {
		return &api.Project{Name: name}, "", nil
	}

	inst := testlib.NewInstance("p", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	p.state.setListed("p")
	p.reads.flush(t.Context())
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, api.StatusErrorf(404, "not found")
	}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}

	commands := make(chan iutil.Command, 4)
	p.args.CommandOut = commands

	var handed []*iutil.Event

	p.args.Next = func(ev *iutil.Event) { handed = append(handed, ev) }

	release := func() {
		for ev := range p.q.release() {
			p.args.Next(ev)
		}
	}

	ev := func(action, name string, session uint64) *iutil.Event {
		project := ""
		if name != "" {
			project = "p"
		}

		return iutil.NewEvent(time.Now(), action, project, name, "").WithChainState(iutil.ChainCold).WithSession(session)
	}

	p.held.seen(ev(iutil.ActionConnected, "", 2))
	p.sweepSession = 2
	p.accept(t.Context(), ev(api.EventLifecycleInstanceUpdated, inst.Name, 2))
	read := awaitResult(t, p)

	p.Handle(ev("backlog", "", 2))
	p.Handle(ev(iutil.ActionConnected, "", 3))
	p.Handle(ev(api.EventLifecycleInstanceUpdated, "other", 3))

	p.settleRead(t.Context(), read)
	release()

	<-p.inbox
	require.Len(t, p.held.take(), 1)
	p.sweepSession = 3

	// Session 3's run breaks on the listing of p, past the grace.
	p.opts.roundGrace = 0
	p.acceptSweep(t.Context(), sweepMsg{
		action: sweepActionBroken,
		err:    &listingError{what: listedInstances, project: "p", err: errors.New("timeout")},
		run:    p.sweepRun,
	})
	release()

	require.Len(t, commands, 1, "precondition: the round of session 3 ended by grace")

	deleted := false

	for _, e := range handed {
		if e.Name() == inst.Name && e.Action() == api.EventLifecycleInstanceDeleted && e.Session() >= 3 {
			deleted = true
		}
	}

	require.True(t, deleted,
		"the round of session 3 ended warm with web still answered: the 404 that found it gone was a leftover, and changed nothing (held=%v, retries=%d)",
		p.state.instance("p", inst.Name) != nil, len(p.retries.due))
}
