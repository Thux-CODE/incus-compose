package enricher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
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

// TestThirdProjectFilterAfterRecentInstanceRead: the predicate leaves shop
// out of the run, and an instance of shop is read between the listing and its
// delivery. The project is not served, and the read does not put it back.
func TestThirdProjectFilterAfterRecentInstanceRead(t *testing.T) {
	p := bare(t)
	fleet := testlib.NewProject("shop", 1, 0)
	f := &fixture{fleet: fleet}
	msgs := make(chan sweepMsg, 1)
	served, _, err := sweepProjects(t.Context(), slog.Default(), sweepArgs{
		conn: f, serves: func(*api.Project) bool { return false }, timeout: time.Second, sweepOut: msgs,
	})
	require.NoError(t, err)
	require.Empty(t, served)
	inst := fleet.Instances[0]
	p.reads.flush(t.Context())
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, nil, nil
	}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "shop", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	require.NotNil(t, p.state.instance("shop", inst.Name))
	p.acceptSweep(t.Context(), <-msgs)
	require.Nil(t, p.state.instance("shop", inst.Name), "the project was explicitly excluded by its configuration")
}

// TestThirdStoredProjectWithNoInstanceListing: the cold store was written
// after shop's configuration was read and before its instances were ever
// listed. That is no inventory: a listing of them that keeps failing does not
// end the run.
func TestThirdStoredProjectWithNoInstanceListing(t *testing.T) {
	s := newState()
	s.setProject("shop", map[string]string{})
	data, err := json.Marshal(s)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "fleet.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	p := New(slog.Default(), StoreFile(path))
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedInstances, project: "shop", err: errors.New("unavailable")})
	require.Empty(t, commands, "no instance inventory was ever obtained")
}

// TestThirdStoredEmptyListingIsAnInventory: the other way round, shop listed
// with no instance in it and stored. That is an inventory, and the run ends
// on it after the grace.
func TestThirdStoredEmptyListingIsAnInventory(t *testing.T) {
	s := newState()
	s.setProject("shop", map[string]string{})
	s.project("shop").listed = true
	data, err := json.Marshal(s)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "fleet.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	p := New(slog.Default(), StoreFile(path))
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedInstances, project: "shop", err: errors.New("unavailable")})
	require.Len(t, commands, 1, "an empty listing stored is an inventory")
}

// TestThirdRecreatedProjectHasNoPreviousListing: shop was listed, then a
// projects listing pruned it, then a project shop was made again. The old
// listing says nothing about the new one.
func TestThirdRecreatedProjectHasNoPreviousListing(t *testing.T) {
	p := bare(t)
	p.state.setProject("shop", nil)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "shop", at: time.Now()})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, at: time.Now()})
	require.Empty(t, p.state.projects)
	p.state.setProject("shop", nil)
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedInstances, project: "shop", err: errors.New("unavailable")})
	require.Empty(t, commands, "the deleted project's inventory says nothing about its replacement")
}

// TestThirdKnownEmptyProjectInventory: a run lists no served project at all
// and ends. Then the projects listing keeps failing: the last one that
// answered was empty, which is an inventory, and the run ends on it.
func TestThirdKnownEmptyProjectInventory(t *testing.T) {
	p := bare(t)
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, at: time.Now()})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	require.Len(t, commands, 1)
	<-commands
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedProjects, err: errors.New("unavailable")})
	require.Len(t, commands, 1, "the last successful listing was empty")
}

// TestThirdColdProjectListingGraceReleasesEnd: after a restart an update is
// held back for the run's networks, and the projects listing keeps failing, so
// no run gets that far. Past the grace the end is raised, and it reaches the
// chain: the held update fails ahead of it rather than holding it in line.
//
// Adapted: the projects counted as listed are now projectsListed, where the
// review set the map it replaced.
func TestThirdColdProjectListingGraceReleasesEnd(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.state.projectsListed = true
	p.sweepSession = 2
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "").WithSession(2))
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionBroken, err: &listingError{what: listedProjects, err: errors.New("unavailable")}})
	require.Len(t, commands, 1)
	cmd := <-commands
	p.accept(t.Context(), iutil.NewEvent(time.Now(), cmd.Action, "", "", "").WithSession(cmd.Session).WithChainState(cmd.ChainState))
	var forwarded []string
	for ev := range p.q.release() {
		forwarded = append(forwarded, ev.Action())
	}
	require.Contains(t, forwarded, iutil.ActionSweepEnd)
	require.True(t, p.reads.warm, "reads no longer wait on a pass that is not coming")
}

// TestThirdSyntheticEventAfterPriorSessionDelivery: a warm event of session 1
// arrives late, after the run was started cold for session 2. The update the
// enricher mints next is stamped as session 2 stands - cold - and as session 2.
func TestThirdSyntheticEventAfterPriorSessionDelivery(t *testing.T) {
	p := bare(t)
	p.sweepSession = 2
	p.chain = iutil.ChainCold
	p.accept(t.Context(), iutil.NewEvent(time.Now(), "earlier", "", "", "").WithSession(1).WithChainState(iutil.ChainWarm))
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", "web", ""))
	for ev := range p.q.release() {
		if ev.Name() == "web" {
			require.Equal(t, iutil.ChainCold, ev.ChainState(), "the run in session two is still cold")
			require.Equal(t, uint64(2), ev.Session(), "minted here for the session read for")
			return
		}
	}
	t.Fatal("the un-enriched update was never forwarded")
}

// TestThirdDiscardedNetworkHistory: networks deleted under names never seen
// again, then a listing of the project asked for after them, with no network
// read out. Nothing is left to tell older from newer, and the history goes.
func TestThirdDiscardedNetworkHistory(t *testing.T) {
	p := bare(t)
	for _, name := range []string{"build-a", "build-b", "build-c"} {
		p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkDeleted, "p", name, ""))
	}
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "p", at: time.Now()})
	require.Empty(t, p.reads.calls)
	require.Empty(t, p.netMoved, "the fresh listing settled every deletion and no older read remains")
}

// TestThirdNetworkHistoryKeptForAnOlderRead: the same with a network read out,
// asked for before the delete under a tenant's key (the delete abandons one
// under its own). The history stays, so that read lands as older news; a
// listing taken before the delete settles nothing either.
func TestThirdNetworkHistoryKeptForAnOlderRead(t *testing.T) {
	p := bare(t)
	n := api.Network{Name: "net", Project: "p", Managed: true, Config: map[string]string{}}
	p.reads.readNet = func(context.Context, string, string) (*api.Network, string, error) { return &n, "", nil }
	before := time.Now()
	p.readNetwork(t.Context(), "tenant", "net")
	res := awaitResult(t, p)
	p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkDeleted, "p", "net", ""))

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "p", at: before})
	require.NotEmpty(t, p.netMoved, "a listing from before the delete settled it")

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "p", at: time.Now()})
	require.NotEmpty(t, p.netMoved, "settled with a read from before it still out")

	p.settleRead(t.Context(), res)
	_, _, exists := p.state.network("p", "net")
	require.False(t, exists, "the older read put the network back")
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
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithSession(2))
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(3))
	p.Handle(iutil.NewEvent(time.Now(), "lost", "", "", ""))

	require.Equal(t, []string{"lost"}, dropped)

	// Held by session: what session 2 said is a leftover once 3 is held.
	held := p.held.take()
	require.Len(t, held, 1)
	require.Equal(t, iutil.ActionConnected, held[0].Action())
	require.Equal(t, uint64(3), held[0].Session())

	// And one of session 2 coming after it is not held at all.
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithSession(2))
	require.Empty(t, p.held.take())
	require.Equal(t, []string{"lost"}, dropped, "a leftover control goes nowhere")
}
