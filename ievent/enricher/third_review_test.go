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

// TestThirdRoundSessionAfterMissedConnected: the connected of session 2 is
// not taken at the inbox, and the next event of session 2 is. The run is
// started for session 2 all the same, and its end names it.
func TestThirdRoundSessionAfterMissedConnected(t *testing.T) {
	p := bare(t)
	p.reads.fleet = &fixture{fleet: testlib.NewProject("p", 0, 0)}
	t.Cleanup(func() {
		if p.sweeperCancel != nil {
			p.sweeperCancel()
		}
	})
	p.inbox = make(chan *iutil.Event, 1)
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.sweepSession = 1
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithSession(1))
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(2))
	p.accept(t.Context(), <-p.inbox)
	p.Handle(iutil.NewEvent(time.Now(), "observed", "", "", "").WithSession(2))
	p.accept(t.Context(), <-p.inbox)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})
	require.Equal(t, uint64(2), (<-commands).Session)
}

// TestThirdOldListingWhileReplacementReadIsPending: a 404 leaves a delete
// owed while the read after it is out; a listing taken before that read
// arrives leaving the instance out. The read finds the replacement, and what
// reaches the chain last about it is not a delete.
func TestThirdOldListingWhileReplacementReadIsPending(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	old := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	gone := api.StatusErrorf(404, "gone")
	reads := 0
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		reads++
		if reads == 1 {
			return nil, nil, gone
		}
		return &inst, nil, nil
	}
	p.reads.flush(t.Context())
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	first := awaitResult(t, p)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	require.Len(t, p.owedDelete, 1)
	p.acceptSweep(t.Context(), old)
	p.settleRead(t.Context(), awaitResult(t, p))
	var actions []string
	for e := range p.q.release() {
		actions = append(actions, e.Action())
	}
	require.Contains(t, actions, api.EventLifecycleInstanceUpdated)
	require.NotEqual(t, api.EventLifecycleInstanceDeleted, actions[len(actions)-1])
	require.Empty(t, p.owedDelete, "the read that found the replacement settled it")
}

// TestThirdConnectedIsNeverDropped: a full inbox drops what it has no room
// for, but for a connected, which waits in its slot - the newest of them -
// instead of going on dropped.
func TestThirdConnectedIsNeverDropped(t *testing.T) {
	p := bare(t)
	p.inbox = make(chan *iutil.Event, 1)

	var dropped []string

	p.args.Next = func(ev *iutil.Event) { dropped = append(dropped, ev.Action()) }

	p.Handle(iutil.NewEvent(time.Now(), "occupied", "", "", ""))
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(2))
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(3))
	p.Handle(iutil.NewEvent(time.Now(), "lost", "", "", ""))

	require.Equal(t, []string{"lost"}, dropped)
	require.Equal(t, uint64(3), (<-p.reconnect).Session())
}
