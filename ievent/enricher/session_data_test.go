package enricher

import (
	"context"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestAnOlderSessionsDeleteChangesNothing: session 2 is up when a delete of
// session 1 comes late out of a queue. It goes on, but takes nothing away
// here: the instance is still held, still archived.
func TestAnOlderSessionsDeleteChangesNothing(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)

	var handed []*iutil.Event

	p.args.Next = func(ev *iutil.Event) { handed = append(handed, ev) }

	// Session 2 up, without starting a run over a fleet this test has none of.
	p.sweepSession = 2
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceDeleted, "p", inst.Name, "").WithChainState(iutil.ChainCold).WithSession(1))

	for ev := range p.q.release() {
		p.args.Next(ev)
	}

	require.NotNil(t, p.state.instance("p", inst.Name), "an older session's delete took the instance away")
	require.Equal(t, api.EventLifecycleInstanceDeleted, handed[len(handed)-1].Action(), "the leftover still goes on")
}

// TestAReadOfAnOlderSessionIsNotArchived: an update read in session 1 lands
// after session 2 came up. Its event goes on, of session 1, and nothing after
// this plugin folds it; the archive does not take it as handed on either, so
// the same answer read again in session 2 goes on.
func TestAReadOfAnOlderSessionIsNotArchived(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, nil, nil
	}
	p.reads.flush(t.Context())

	update := func(session uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "").WithChainState(iutil.ChainCold).WithSession(session)
	}

	// The sessions set by hand, without starting a run over a fleet this test
	// has none of.
	p.sweepSession = 1
	p.accept(t.Context(), update(1))
	read := awaitResult(t, p)
	p.sweepSession = 2
	p.settleRead(t.Context(), read)

	var sessions []uint64

	for ev := range p.q.release() {
		if ev.Instance() != nil {
			sessions = append(sessions, ev.Session())
		}
	}

	require.Equal(t, []uint64{1}, sessions)

	p.accept(t.Context(), update(2))
	p.settleRead(t.Context(), awaitResult(t, p))

	sessions = nil

	for ev := range p.q.release() {
		if ev.Instance() != nil {
			sessions = append(sessions, ev.Session())
		}
	}

	require.Equal(t, []uint64{2}, sessions, "the same answer, in the newer session, was held back as nothing new")
}
