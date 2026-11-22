package enricher

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestNinthLeftoverNotFoundBeforeRecreatedInstance: web is held. A read of
// session 2 goes out after Incus deleted it and before it was made again,
// and lands as a leftover with a 404, after the door has seen 3 and the new
// web's event of session 3 has asked for a read of its own. The 404 takes the
// normal path: the delete is owed by the key, and the newer read finding the
// new web settles it. No delete goes on, and the new web goes on in session 3.
func TestNinthLeftoverNotFoundBeforeRecreatedInstance(t *testing.T) {
	p := New(slog.Default(), InboxSize(1))
	require.NoError(t, p.reads.start(1))
	t.Cleanup(p.reads.stop)

	p.reads.readProject = func(_ context.Context, name string) (*api.Project, string, error) {
		return &api.Project{Name: name}, "", nil
	}

	inst := testlib.NewInstance("p", 0, 0)
	state := testlib.NewInstanceState(0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, state)
	p.reads.flush(t.Context())

	var reads atomic.Int32

	release1 := make(chan struct{})

	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		if reads.Add(1) == 1 {
			<-release1

			return nil, nil, api.StatusErrorf(404, "not found")
		}

		return &inst, state, nil
	}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}

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

	p.Handle(ev("backlog", "", 2))
	p.Handle(ev(iutil.ActionConnected, "", 3))
	p.Handle(ev(api.EventLifecycleInstanceUpdated, "other", 3))
	<-p.inbox
	require.Len(t, p.held.take(), 1)
	p.sweepSession = 3

	// The new web's event, while the old read is still out.
	p.accept(t.Context(), ev(api.EventLifecycleInstanceUpdated, inst.Name, 3))
	close(release1)

	p.settleRead(t.Context(), awaitResult(t, p))
	release()
	p.settleRead(t.Context(), awaitResult(t, p))
	release()

	var deleted, served bool

	for _, e := range handed {
		if e.Name() != inst.Name {
			continue
		}

		if e.Action() == api.EventLifecycleInstanceDeleted {
			deleted = true
		}

		if e.Instance() != nil && e.Err() == nil && e.Session() >= 3 {
			served = true
		}
	}

	require.False(t, deleted, "the old 404 deleted a web made again since")
	require.True(t, served, "the new web never went on in session 3")
	require.NotNil(t, p.state.instance("p", inst.Name))
}
