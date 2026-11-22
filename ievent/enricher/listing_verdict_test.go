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

// TestARoundWaitsForAReadNewerThanItsListing: the read out went after the
// listing that left the instance out, so it decides - and the end of the
// round waits for it as well, or the round would end with the instance still
// held downstream until its 404 lands.
func TestARoundWaitsForAReadNewerThanItsListing(t *testing.T) {
	p := bare(t)
	commands := make(chan iutil.Command, 4)
	p.args.CommandOut = commands

	var deleted bool

	p.args.Next = func(ev *iutil.Event) {
		if ev.Action() == api.EventLifecycleInstanceDeleted {
			deleted = true
		}
	}

	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, api.StatusErrorf(404, "gone")
	}
	p.reads.flush(t.Context())

	listing := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}

	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	read := awaitResult(t, p)
	require.True(t, read.call.sent.After(listing.at))

	p.acceptSweep(t.Context(), listing)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	require.Empty(t, commands, "the round ended before the read that decides")

	p.settleRead(t.Context(), read)

	for ev := range p.q.release() {
		p.args.Next(ev)
	}

	require.Len(t, commands, 1)
	require.True(t, deleted)
	require.Nil(t, p.state.instance("p", inst.Name))
}

// TestALeftoverReadNewerThanTheListingTakesTheVerdictBack: a listing leaves
// out a held instance while a read of it is out, so its verdict waits. That
// read went before the listing and decides nothing; the one asked for after
// it finds the instance alive, but lands as a leftover, the door having seen
// session 3 meanwhile. It is still news that the instance outlived the
// listing: the verdict is taken back, and no delete goes on.
func TestALeftoverReadNewerThanTheListingTakesTheVerdictBack(t *testing.T) {
	p := bare(t)

	var deleted bool

	p.args.Next = func(ev *iutil.Event) {
		if ev.Action() == api.EventLifecycleInstanceDeleted {
			deleted = true
		}
	}

	inst := testlib.NewInstance("p", 0, 0)
	state := testlib.NewInstanceState(0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, state)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, state, nil
	}
	p.reads.flush(t.Context())

	update := func() *iutil.Event {
		return iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "").WithChainState(iutil.ChainCold).WithSession(2)
	}

	p.held.seen(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(2))
	p.sweepSession = 2

	p.accept(t.Context(), update())
	older := awaitResult(t, p)

	listing := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	p.acceptSweep(t.Context(), listing)
	require.True(t, p.verdictsOwed(), "the verdict waits for the read out")

	// A read asked for after the listing, behind the one out.
	p.accept(t.Context(), update())
	p.settleRead(t.Context(), older)
	require.True(t, p.verdictsOwed(), "the older read decides nothing")

	newer := awaitResult(t, p)
	require.True(t, newer.call.sent.After(listing.at))

	// Session 3 seen at the door before the newer read lands.
	p.held.seen(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(3))
	p.settleRead(t.Context(), newer)

	for ev := range p.q.release() {
		p.args.Next(ev)
	}

	require.False(t, deleted, "a live instance found after the listing was deleted")
	require.False(t, p.verdictsOwed())
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
