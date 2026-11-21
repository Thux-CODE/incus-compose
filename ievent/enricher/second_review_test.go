package enricher

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestSecondDeleteObligationSurvivesReadError: 404 with a read owed, the owed
// read times out, the retry finds 404 again. The delete the first 404 handed
// on is still owed, and that retry announces it.
func TestSecondDeleteObligationSurvivesReadError(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	gone := api.StatusErrorf(http.StatusNotFound, "gone")
	results := []error{gone, errors.New("timeout"), gone}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		err := results[0]
		results = results[1:]
		return nil, nil, err
	}
	p.reads.flush(t.Context())
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	first := <-p.reads.results
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	p.settleRead(t.Context(), <-p.reads.results)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), <-p.reads.results)
	for ev := range p.q.release() {
		if ev.Action() == api.EventLifecycleInstanceDeleted {
			return
		}
	}
	t.Fatal("the subscriber still has the former instance after two 404s and a retry")
}

// TestSecondOwedDeleteByTheListing: the same debt with every read failing
// after the first 404. The listing that no longer has the instance is what
// announces it.
func TestSecondOwedDeleteByTheListing(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	gone := api.StatusErrorf(http.StatusNotFound, "gone")
	results := []error{gone, errors.New("timeout")}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		err := results[0]
		results = results[1:]
		return nil, nil, err
	}
	p.reads.flush(t.Context())
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	first := <-p.reads.results
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	p.settleRead(t.Context(), <-p.reads.results)

	var deleted bool

	p.args.Next = func(ev *iutil.Event) {
		deleted = deleted || (ev.Action() == api.EventLifecycleInstanceDeleted && ev.Name() == inst.Name)
	}

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", run: p.sweepRun})

	require.True(t, deleted, "a listing without the instance announced no delete")
	require.Empty(t, p.owedDelete)
}

// TestSecondReadOutsideListingCannotHoldRound: the listings are done and one
// project read is still out when a live read of an instance no listing named
// fails. The run now waits on it - with the grace armed, so it ends anyway.
func TestSecondReadOutsideListingCannotHoldRound(t *testing.T) {
	p := New(slog.Default(), func(o *options) { o.roundGrace = 50 * time.Millisecond })
	p.args.Next = func(*iutil.Event) {}
	p.reads.readProject = func(_ context.Context, name string) (*api.Project, string, error) {
		return &api.Project{Name: name}, "", nil
	}
	require.NoError(t, p.reads.start(3))
	t.Cleanup(p.reads.stop)
	raised := make(chan iutil.Command, 4)
	p.args.CommandOut = raised
	p.reads.flush(t.Context())
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, errors.New("timeout")
	}
	// One listed project read is finished by the worker, and awaits settlement.
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"p", "q"}})
	proj := <-p.reads.results
	p.settleRead(t.Context(), proj)
	other := <-p.reads.results
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: proj.call.project})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: other.call.project})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, proj.call.project, "live", ""))
	p.settleRead(t.Context(), <-p.reads.results)
	p.settleRead(t.Context(), other)
	select {
	case cmd := <-raised:
		require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
	default:
		require.NotNil(t, p.endGrace, "round waits, with no grace timer armed")
	}

	// And the grace is what ends it, the read failing for good.
	<-p.endGrace
	p.endGrace = nil
	p.graceOver = true
	p.endIfRead(t.Context())
	require.Equal(t, iutil.ActionSweepEnd, (<-raised).Action)
}

// TestSecondRoundEndNamesItsSession: the run a connected restarts ends naming
// that connected's session, which is what the source checks it against.
func TestSecondRoundEndNamesItsSession(t *testing.T) {
	p := bare(t)
	p.reads.fleet = &fixture{}

	raised := make(chan iutil.Command, 1)
	p.args.CommandOut = raised

	p.accept(t.Context(), iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(7))
	t.Cleanup(p.sweeperCancel)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})

	cmd := <-raised
	require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
	require.Equal(t, uint64(7), cmd.Session)
}
