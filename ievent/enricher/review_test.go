package enricher

import (
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestReviewOld404AfterNewRead: a read finds the instance gone, while an update
// for the instance made again under the same name waits behind it for a read
// of its own. That read finds the new instance, and nothing after it takes the
// name away: the 404 was older news. Three events reach the chain - the
// update the 404 answered, the one the second read answered, the sync - and
// the last about the name is the update.
func TestReviewOld404AfterNewRead(t *testing.T) {
	h := seeded(t, 1, 1)
	name := testlib.InstanceName(0)
	before := h.readsOf("p", name)
	h.mu.Lock()
	h.err = incusapi.StatusErrorf(http.StatusNotFound, "absent at time of read")
	gate := make(chan struct{})
	h.gate = gate
	h.mu.Unlock()
	h.send(incusapi.EventLifecycleInstanceUpdated, "p", name)
	require.Eventually(t, func() bool { return h.readsOf("p", name) > before }, time.Second, time.Millisecond)
	h.mu.Lock()
	h.err = nil
	h.tag = "replacement"
	h.mu.Unlock()
	h.send(incusapi.EventLifecycleInstanceUpdated, "p", name)
	settled(t, h)
	close(gate)
	var last *iutil.Event
	for range 3 {
		e := h.next()
		if e.Name() == name {
			last = e
		}
	}
	require.NotNil(t, last)
	require.Equal(t, incusapi.EventLifecycleInstanceUpdated, last.Action(), "the recreated instance must remain published")
	require.True(t, last.Enriched(iutil.EnrichedInstance))
	require.Equal(t, "replacement", tagOf(last))

	select {
	case e := <-h.out:
		require.NotEqual(t, name, e.Name(), "%s after the read of the new instance", e.Action())
	case <-time.After(200 * time.Millisecond):
	}
}

// TestReviewOld404ThenGoneAgain: the same, with the instance gone again by the
// second read. The delete the first read owed is the second read's to make.
func TestReviewOld404ThenGoneAgain(t *testing.T) {
	h := seeded(t, 1, 1)
	name := testlib.InstanceName(0)
	before := h.readsOf("p", name)
	h.mu.Lock()
	h.err = incusapi.StatusErrorf(http.StatusNotFound, "absent at time of read")
	gate := make(chan struct{})
	h.gate = gate
	h.mu.Unlock()
	h.send(incusapi.EventLifecycleInstanceUpdated, "p", name)
	require.Eventually(t, func() bool { return h.readsOf("p", name) > before }, time.Second, time.Millisecond)
	h.send(incusapi.EventLifecycleInstanceUpdated, "p", name)
	settled(t, h)
	close(gate)

	var last string
	for range 4 {
		e := h.next()
		if e.Name() == name {
			last = e.Action()
		}
	}
	require.Equal(t, incusapi.EventLifecycleInstanceDeleted, last)
}

// TestReviewSweepEndsDespiteReadError: a run whose listings succeed and whose
// read of a listed instance fails is not one that read the fleet whole, so it
// does not end - until the read is tried again and lands, which is what ends
// it then.
func TestReviewSweepEndsDespiteReadError(t *testing.T) {
	h := newFixture(t)
	h.mu.Lock()
	h.err = errors.New("read timeout")
	h.mu.Unlock()
	fleet := testlib.NewProject("p", 1, 1)
	fleet.Project.Config[iutil.FeaturesNetworks] = "true"
	h.setFleet(fleet)
	seen := h.collect(2)
	require.Equal(t, iutil.ActionConnected, seen[0].Action())
	require.Error(t, seen[1].Err())

	select {
	case ev := <-h.out:
		require.NotEqual(t, iutil.ActionSweepEnd, ev.Action(), "first full read is still missing an instance")
	case <-time.After(300 * time.Millisecond):
	}

	h.mu.Lock()
	h.err = nil
	h.mu.Unlock()

	untilSweepEnd(t, h, 10*time.Second)
}

// TestReviewSweepEndsAfterTheGrace: a read that keeps failing holds the end of
// a run for the grace and no longer, or one instance would keep the chain cold
// for good.
func TestReviewSweepEndsAfterTheGrace(t *testing.T) {
	h := newFixture(t, func(o *options) { o.roundGrace = 200 * time.Millisecond })
	h.mu.Lock()
	h.err = errors.New("read timeout")
	h.mu.Unlock()
	fleet := testlib.NewProject("p", 1, 1)
	fleet.Project.Config[iutil.FeaturesNetworks] = "true"
	h.setFleet(fleet)
	start := time.Now()
	untilSweepEnd(t, h, 5*time.Second)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
}

// TestAnAbandonedRunEndsNothing: a reconnect abandons the run under way. What
// it still gets through - its end above all, held on a project read or not -
// says nothing about the fleet since the stream came back, and is not taken.
func TestAnAbandonedRunEndsNothing(t *testing.T) {
	p := bare(t)
	raised := make(chan iutil.Command, 4)
	p.args.CommandOut = raised

	stale := p.sweepArgs().run
	p.sweepRun++

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: stale})
	require.Empty(t, raised, "the end of an abandoned run was announced")

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})
	require.Equal(t, iutil.ActionSweepEnd, (<-raised).Action)
}

// untilSweepEnd reads the chain until the end of a run.
func untilSweepEnd(t *testing.T, h *fixture, within time.Duration) {
	t.Helper()

	deadline := time.After(within)

	for {
		select {
		case ev := <-h.out:
			if ev.Action() == iutil.ActionSweepEnd {
				return
			}
		case <-deadline:
			t.Fatal("the run never ended")
		}
	}
}

// TestReviewNetworkEventDuringRead: a network read taken before an update, a
// delete or a rename lands after it. The newer event wins: the update is read
// again, and the read of a network that went is abandoned rather than putting
// the old key back.
func TestReviewNetworkEventDuringRead(t *testing.T) {
	for _, action := range []string{incusapi.EventLifecycleNetworkUpdated, incusapi.EventLifecycleNetworkDeleted, incusapi.EventLifecycleNetworkRenamed} {
		t.Run(action, func(t *testing.T) {
			p := New(slog.Default())
			h := &fixture{reads: map[string]int{}, fleet: testlib.NewProject("p", 0, 1)}
			n := &h.fleet.Networks[0]
			n.Managed = true
			n.Config["ipv4.address"] = "10.1.0.1/24"
			oldName := n.Name
			p.reads.readNet = h.answerNet
			require.NoError(t, p.reads.start(2))
			defer p.reads.stop()
			ctx := t.Context()
			p.readNetwork(ctx, "p", oldName)
			old := awaitResult(t, p)
			require.NoError(t, old.err)
			// Keep an API reply as actually captured before the config update.
			snapshot := *old.network
			snapshot.Config = maps.Clone(snapshot.Config)
			old.network = &snapshot
			newName, previous := oldName, ""
			if action == incusapi.EventLifecycleNetworkRenamed {
				newName, previous = "newnet", oldName
				n.Name = newName
			}
			n.Config["ipv4.address"] = "10.2.0.1/24"
			p.acceptNetwork(ctx, iutil.NewEvent(time.Now(), action, "p", newName, previous))
			p.settleRead(ctx, old)

			// The update is read again, and has to land; a delete reads
			// nothing, and a rename reads the new name, which is not asked.
			switch action {
			case incusapi.EventLifecycleNetworkUpdated:
				p.settleRead(ctx, awaitResult(t, p))
			default:
				refresh, landed := maybeResult(ctx, p, 60*time.Millisecond)
				if landed {
					p.settleRead(ctx, refresh)
				}
			}
			net, _, exists := p.state.network("p", oldName)
			if action == incusapi.EventLifecycleNetworkUpdated {
				require.True(t, exists)
				require.Equal(t, "10.2.0.1/24", net.IPv4())
			} else {
				require.False(t, exists, "old network returned to the inventory")
			}
		})
	}
}
