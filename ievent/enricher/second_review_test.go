package enricher

import (
	"context"
	"errors"
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
