package enricher

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestSeventhStaleReadKeepsNewInstanceOutOfUnread: the case of
// TestSixthReadSettledAfterNewerSessionWentDownstream, for an instance
// nothing held before. Its read of session 2 lands while the door has seen
// 3: the event goes on stamped 2, which ic-dns folds nothing of, and stays
// out of the archive (cb0ee68) - but patchState has already put the instance
// in the state. Session 3's listing has it, finds it held and does not make
// the end of the round wait on it. Its read in session 3 fails, a held
// instance is "answered from what is held", and the round ends warm with a
// name nobody after the enricher has: NXDOMAIN, authoritative, until a retry
// lands.
func TestSeventhStaleReadKeepsNewInstanceOutOfUnread(t *testing.T) {
	p := New(slog.Default(), InboxSize(1))
	require.NoError(t, p.reads.start(1))
	t.Cleanup(p.reads.stop)

	p.reads.readProject = func(_ context.Context, name string) (*api.Project, string, error) {
		return &api.Project{Name: name}, "", nil
	}

	inst := testlib.NewInstance("p", 0, 0)
	state := testlib.NewInstanceState(0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.reads.flush(t.Context())

	var fail atomic.Bool

	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		if fail.Load() {
			return nil, nil, errors.New("timeout")
		}

		return &inst, state, nil
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

	// Session 3's run: the listing has the instance, then its read fails.
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", names: []string{inst.Name}, at: time.Now(), run: p.sweepRun})
	fail.Store(true)
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	release()

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})

	downstream := false

	for _, e := range handed {
		if e.Name() == inst.Name && e.Instance() != nil && e.Err() == nil && e.Session() >= 3 {
			downstream = true
		}
	}

	require.False(t, downstream, "precondition: nothing after the enricher has the instance in session 3")
	require.Empty(t, commands,
		"the round of session 3 ended with a listed instance nobody downstream has: it was held in the state by a read of session 2 that went on as a leftover")
}

// TestSeventhUnreadControl: the same round of session 3 without the read of
// session 2 before it. The listed instance nothing holds is unread, and the
// end waits on it (until the grace). Passes: what changes the outcome above
// is only the state the leftover read filled.
func TestSeventhUnreadControl(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.reads.flush(t.Context())
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, errors.New("timeout")
	}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}

	commands := make(chan iutil.Command, 4)
	p.args.CommandOut = commands
	p.sweepSession = 3

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", names: []string{inst.Name}, at: time.Now(), run: p.sweepRun})
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})

	require.Empty(t, commands)
}
