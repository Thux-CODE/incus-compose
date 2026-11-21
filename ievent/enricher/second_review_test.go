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

// awaitResult is the next read to land, served the way Run serves the pool. A
// test driving the plugin by hand has to: a read the pool refuses - its one
// worker not back yet from handing on the last result - waits for the pool's
// timer, which only Run, or this, takes.
func awaitResult(t *testing.T, p *Plugin) result {
	t.Helper()

	res, landed := maybeResult(t.Context(), p, 5*time.Second)
	if !landed {
		t.Fatal("no read landed")
	}

	return res
}

// maybeResult is the next read to land within d, served as awaitResult does,
// and whether one did.
func maybeResult(ctx context.Context, p *Plugin, d time.Duration) (result, bool) {
	deadline := time.After(d)

	for {
		select {
		case res := <-p.reads.results:
			return res, true
		case <-p.reads.timer.C:
			p.reads.retry(ctx)
		case <-deadline:
			return result{}, false
		}
	}
}

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
	first := awaitResult(t, p)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	p.settleRead(t.Context(), awaitResult(t, p))
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), awaitResult(t, p))
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
	first := awaitResult(t, p)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	p.settleRead(t.Context(), awaitResult(t, p))

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
	proj := awaitResult(t, p)
	p.settleRead(t.Context(), proj)
	other := awaitResult(t, p)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: proj.call.project})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: other.call.project})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, proj.call.project, "live", ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	p.settleRead(t.Context(), other)
	select {
	case cmd := <-raised:
		require.Equal(t, iutil.ActionSweepEnd, cmd.Action)

		return
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

// TestSecondNetworkRepliesAfterDelete: a network read taken before its delete
// lands after it, from the sweeper or from a read a tenant's NIC asked for
// under the tenant's key. Neither puts the network back.
func TestSecondNetworkRepliesAfterDelete(t *testing.T) {
	for _, path := range []string{"sweeper", "inherited network read"} {
		t.Run(path, func(t *testing.T) {
			p := bare(t)
			f := &fixture{reads: map[string]int{}, fleet: testlib.NewProject("default", 0, 1)}
			n := f.fleet.Networks[0]
			n.Managed = true
			var res result
			read := time.Now()
			if path == "inherited network read" {
				p.reads.readNet = func(context.Context, string, string) (*api.Network, string, error) { return &n, "", nil }
				p.readNetwork(t.Context(), "tenant", n.Name)
				res = awaitResult(t, p)
			}
			p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkDeleted, "default", n.Name, ""))
			if path == "sweeper" {
				p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetwork, network: &n, at: read})
			} else {
				p.settleRead(t.Context(), res)
			}
			_, _, exists := p.state.network("tenant", n.Name)
			require.False(t, exists, "deleted network was published under its old key")

			// A read asked for after the delete is news, and is taken.
			p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetwork, network: &n, at: time.Now()})
			_, _, exists = p.state.network("default", n.Name)
			require.True(t, exists, "a read after the event was not taken")
		})
	}
}

// TestSecondNetworkCreatedAfterListing: a network listing taken before a
// network was created, delivered after the create's read: it prunes nothing
// the event brought in.
func TestSecondNetworkCreatedAfterListing(t *testing.T) {
	p := bare(t)
	listed := time.Now()
	n := api.Network{Name: "new", Project: "default", Managed: true, Config: map[string]string{"ipv4.address": "10.9.0.1/24"}}
	p.reads.readNet = func(context.Context, string, string) (*api.Network, string, error) { return &n, "", nil }
	p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkCreated, "default", "new", ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	_, _, exists := p.state.network("default", "new")
	require.True(t, exists)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "default", at: listed})
	_, _, exists = p.state.network("default", "new")
	require.True(t, exists, "a listing older than the create pruned it")

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "default", at: time.Now()})
	_, _, exists = p.state.network("default", "new")
	require.False(t, exists, "a listing after the create no longer has it, and prunes it")
}

// TestSecondInstanceCreatedAfterListing: the project's listing captured no
// instance; before it is delivered an instance is started and read. The
// listing is older news, and synthesizes no delete for it. A listing asked
// for after the read, still leaving it out, does.
func TestSecondInstanceCreatedAfterListing(t *testing.T) {
	p := bare(t)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceStarted: {Enrich: iutil.EnrichedInstance}}
	p.reads.flush(t.Context())
	oldList := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	inst := testlib.NewInstance("p", 0, 0)
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, nil, nil
	}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceStarted, "p", inst.Name, ""))
	p.settleRead(t.Context(), awaitResult(t, p))
	events := make([]string, 0)
	p.args.Next = func(ev *iutil.Event) { events = append(events, ev.Action()) }
	for ev := range p.q.release() {
		p.args.Next(ev)
	}
	require.Equal(t, []string{api.EventLifecycleInstanceStarted}, events)
	p.acceptSweep(t.Context(), oldList)
	require.Equal(t, []string{api.EventLifecycleInstanceStarted}, events, "an older list synthesized a delete for the new instance")

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	require.Equal(t, []string{api.EventLifecycleInstanceStarted, api.EventLifecycleInstanceDeleted}, events,
		"a listing after the read leaves it out")
}

// withGrace sets the grace a run held on failures waits, failed reads and
// listings alike.
func withGrace(d time.Duration) Option { return func(o *options) { o.roundGrace = d } }

// brokenFleet is one project with one instance, its networks of its own.
func brokenFleet() *testlib.Project {
	fleet := testlib.NewProject("p", 1, 1)
	fleet.Project.Config[iutil.FeaturesNetworks] = "true"

	return fleet
}

// TestBrokenNetworkListingEndsAfterTheGrace: a network listing that fails for
// good breaks every run, and the run is retried; once a grace has passed since
// the first that broke, the run ends with the inventory there is - and the
// next one that breaks waits a whole grace of its own.
func TestBrokenNetworkListingEndsAfterTheGrace(t *testing.T) {
	h := newFixture(t, sweepRetry(20*time.Millisecond), withGrace(200*time.Millisecond))
	h.mu.Lock()
	h.networksErr = errors.New("network list unavailable")
	h.mu.Unlock()

	start := time.Now()
	h.setFleet(brokenFleet())
	untilSweepEnd(t, h, 5*time.Second)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond, "ended before the grace")

	first := time.Now()
	untilSweepEnd(t, h, 5*time.Second)
	require.GreaterOrEqual(t, time.Since(first), 200*time.Millisecond, "the next run's grace did not start again")
}

// TestBrokenNetworkListingThatRecoversEndsAtOnce: the listing fails once and
// then answers. The retry that lists it ends the run, long before the grace.
func TestBrokenNetworkListingThatRecoversEndsAtOnce(t *testing.T) {
	h := newFixture(t, sweepRetry(20*time.Millisecond), withGrace(time.Minute))
	h.mu.Lock()
	h.networksErr = errors.New("network list unavailable")
	h.mu.Unlock()

	before := h.lists.Load()
	h.setFleet(brokenFleet())
	require.Eventually(t, func() bool { return h.lists.Load() >= before+2 }, 2*time.Second, time.Millisecond,
		"no run broke on the listing")

	h.mu.Lock()
	h.networksErr = nil
	h.mu.Unlock()

	start := time.Now()
	untilSweepEnd(t, h, 5*time.Second)
	require.Less(t, time.Since(start), 5*time.Second)
}

// TestBrokenInstanceListingWithNothingListedStaysCold: a first start, no cold
// store, and the instance listing fails for good. There is nothing to answer
// from: the run does not end, grace or not.
func TestBrokenInstanceListingWithNothingListedStaysCold(t *testing.T) {
	h := newFixture(t, sweepRetry(20*time.Millisecond), withGrace(50*time.Millisecond))
	h.mu.Lock()
	h.instancesErr = errors.New("incusd said no")
	h.mu.Unlock()

	h.setFleet(brokenFleet())

	deadline := time.After(500 * time.Millisecond)

	for {
		select {
		case ev := <-h.out:
			require.NotEqual(t, iutil.ActionSweepEnd, ev.Action(), "a run that never listed the instances ended")
		case <-deadline:
			return
		}
	}
}

// TestBrokenInstanceListingWithAnInventoryEnds: the instances were listed by a
// run that ended; then their listing fails for good. The inventory held is
// what is answered, and after the grace the run ends on it.
func TestBrokenInstanceListingWithAnInventoryEnds(t *testing.T) {
	h := newFixture(t, sweepRetry(20*time.Millisecond), withGrace(200*time.Millisecond))
	h.setFleet(brokenFleet())
	untilSweepEnd(t, h, 5*time.Second)

	h.mu.Lock()
	h.instancesErr = errors.New("incusd said no")
	h.mu.Unlock()

	start := time.Now()
	h.send(iutil.ActionConnected, "", "")
	untilSweepEnd(t, h, 5*time.Second)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond, "ended before the grace")
}

type secondNetworkListFailure struct{ *fixture }

func (f secondNetworkListFailure) GetNetworkNames(context.Context, string) ([]string, error) {
	return nil, errors.New("network list unavailable")
}

// TestSecondFailedNetworkListEndsRound: the network listing fails and the
// instance listing succeeds. The run is not whole, and fails - tried again
// after the backoff rather than called done.
func TestSecondFailedNetworkListEndsRound(t *testing.T) {
	f := &fixture{fleet: testlib.NewProject("p", 0, 0)}
	a := sweepArgs{conn: secondNetworkListFailure{f}, sweepOut: make(chan sweepMsg, 16), timeout: time.Second}
	err := runSweep(t.Context(), slog.Default(), a)
	require.Error(t, err, "runSweeper turns this nil into sweepActionDone")

	// The instances were still swept, so the names held are confirmed or
	// pruned; and once the listing answers the run ends.
	require.NoError(t, runSweep(t.Context(), slog.Default(), sweepArgs{conn: f, readNet: f.answerNet, sweepOut: make(chan sweepMsg, 16), timeout: time.Second}))
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
