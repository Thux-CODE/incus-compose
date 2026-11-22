package enricher

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestARoundWaitsForTheListingsVerdict: a listing leaves out a held instance
// while a read of it that went before the listing is out. The verdict waits
// for that read, and so does the end of the round: ending first would leave
// the instance held downstream, past the round that should have taken it.
func TestARoundWaitsForTheListingsVerdict(t *testing.T) {
	p := bare(t)
	commands := make(chan iutil.Command, 4)
	p.args.CommandOut = commands
	p.args.Next = func(*iutil.Event) {}

	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, errors.New("unavailable")
	}
	p.reads.flush(t.Context())

	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	read := awaitResult(t, p)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	require.NotNil(t, p.state.instance("p", inst.Name), "the verdict waits for the read out")

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	require.Empty(t, commands, "the round ended before the verdict")

	p.settleRead(t.Context(), read)
	require.Nil(t, p.state.instance("p", inst.Name), "the read failed: the listing's delete is final")
	require.Len(t, commands, 1)
	require.Empty(t, p.owedDelete)
}

// TestANewerListingSettlesAnOlderVerdict: a listing that has the instance,
// newer than the one that left it out, takes the verdict back, so it holds no
// round up.
func TestANewerListingSettlesAnOlderVerdict(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, errors.New("unavailable")
	}
	p.reads.flush(t.Context())

	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	awaitResult(t, p)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	require.True(t, p.verdictsOwed())

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", names: []string{inst.Name}, at: time.Now()})
	require.False(t, p.verdictsOwed())
}
