package enricher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestSixthReadSettledAfterNewerSessionWentDownstream: the inbox is full, so
// the connected of session 3 waits in held and the next event of session 3
// goes on dropped, ahead of everything. Downstream (ic-dns, http) now knows
// session 3. A read of session 2 lands before Run takes the held connected:
// the enricher still reads for 2, so it files the answer in the archive and
// hands it on stamped 2 - which ic-dns, already in 3, folds nothing of
// (N4). The run of session 3 then reads the same instance, finds the same
// answer and trashes it as nothing new. No event carrying the instance in
// a session downstream accepts ever goes on: a live instance stays
// NXDOMAIN (or keeps its old address) until its data changes again.
func TestSixthReadSettledAfterNewerSessionWentDownstream(t *testing.T) {
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
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
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

	// Session 2 up (set by hand, as the other tests here do, so no sweeper
	// starts over a fleet this test has none of), and an update of session 2
	// whose read is out.
	p.held.seen(ev(iutil.ActionConnected, "", 2))
	p.sweepSession = 2
	p.accept(t.Context(), ev(api.EventLifecycleInstanceUpdated, inst.Name, 2))
	read := awaitResult(t, p)

	// The inbox is full with a leftover of session 2; session 3 comes up.
	p.Handle(ev("backlog", "", 2))
	p.Handle(ev(iutil.ActionConnected, "", 3))
	p.Handle(ev(api.EventLifecycleInstanceUpdated, "other", 3))

	require.Len(t, handed, 1, "the second event of session 3 goes on dropped")
	require.Equal(t, uint64(3), handed[0].Session())
	require.Error(t, handed[0].Err())

	// What downstream has seen by now.
	downstream := handed[0].Session()

	// The read of session 2 lands before Run gets to the held connected.
	p.settleRead(t.Context(), read)
	release()

	// Run takes the inbox, then what is held: session 3 opens here too. By
	// hand, without the sweeper; then the run's own read of the instance,
	// unstamped as the sweeper sends it.
	<-p.inbox
	held := p.held.take()
	require.Len(t, held, 1)
	require.Equal(t, iutil.ActionConnected, held[0].Action())
	p.sweepSession = 3

	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	release()

	reached, older := false, false

	for _, e := range handed {
		if e.Name() == inst.Name && e.Instance() != nil && e.Err() == nil && e.Session() == 2 {
			older = true
		}

		if e.Name() == inst.Name && e.Instance() != nil && e.Err() == nil && e.Session() >= downstream {
			reached = true
		}
	}

	require.True(t, older, "the read of session 2 went on, stamped 2")
	require.True(t, reached,
		"the instance went on only stamped with a session older than one downstream already had (%d): ic-dns folds none of it, and the archive holds every later read of it back", downstream)
}
