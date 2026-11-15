package dns

import (
	"log/slog"
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"
)

// restoredReplica is replica as the cold store replays it: what a previous run
// last read, address and health included.
func restoredReplica(name, addr, status string) *iutil.Event {
	return replica(incusapi.EventLifecycleInstanceUpdated, name, addr, true, status).WithRestored()
}

// TestReadinessPrefersWhatThisRunRead: a service answers with the ready
// replicas read in this run; where there are none, with the ready ones the cold
// store restored; where there are none of those either, with every candidate.
// A drained replica is never a candidate.
func TestReadinessPrefersWhatThisRunRead(t *testing.T) {
	t.Parallel()

	const (
		started   = incusapi.EventLifecycleInstanceStarted
		healthy   = shared.HealthStatusHealthy
		unhealthy = shared.HealthStatusUnhealthy
		service   = "frontend.shop.example."
	)

	tests := []struct {
		name string
		feed []*iutil.Event
		want []string
	}{
		{
			// A restored address may have moved while the process was down.
			name: "a fresh ready replica outranks a restored ready one",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				restoredReplica("frontend-2", "10.0.0.6", healthy),
			},
			want: []string{"10.0.0.5"},
		},
		{
			name: "a replica nothing checks, read in this run, is a fresh ready one",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", false, ""),
				restoredReplica("frontend-2", "10.0.0.6", healthy),
			},
			want: []string{"10.0.0.5"},
		},
		{
			name: "no fresh ready replica: the restored ready ones",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, shared.HealthStatusStarting),
				restoredReplica("frontend-2", "10.0.0.6", healthy),
				restoredReplica("frontend-3", "10.0.0.7", unhealthy),
			},
			want: []string{"10.0.0.6"},
		},
		{
			// The replay of a fleet nothing has read yet.
			name: "only restored replicas: the ready ones among them",
			feed: []*iutil.Event{
				restoredReplica("frontend-1", "10.0.0.5", healthy),
				restoredReplica("frontend-2", "10.0.0.6", unhealthy),
			},
			want: []string{"10.0.0.5"},
		},
		{
			name: "none ready, fresh or restored: every candidate",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, unhealthy),
				restoredReplica("frontend-2", "10.0.0.6", shared.HealthStatusStarting),
			},
			want: []string{"10.0.0.5", "10.0.0.6"},
		},
		{
			name: "a drained fresh ready replica leaves the restored ones",
			feed: []*iutil.Event{
				drained("frontend-1", "10.0.0.5", healthy),
				restoredReplica("frontend-2", "10.0.0.6", healthy),
			},
			want: []string{"10.0.0.6"},
		},
		{
			name: "a drained restored ready replica is no fallback",
			feed: []*iutil.Event{
				drained("frontend-1", "10.0.0.5", healthy).WithRestored(),
				replica(started, "frontend-2", "10.0.0.6", true, unhealthy),
			},
			want: []string{"10.0.0.6"},
		},
		{
			name: "every replica drained, restored or not: no answer",
			feed: []*iutil.Event{
				drained("frontend-1", "10.0.0.5", healthy),
				drained("frontend-2", "10.0.0.6", healthy).WithRestored(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snap := fed(t, test.feed...).state.snapshot()

			assert.Equal(t, test.want, answered(t, snap, service, "10.0.0.5"))
		})
	}
}

// TestARestoredReplicaReadUnchangedTakesItsPlace: the first round reads the
// restored replicas one by one, and a read that finds one unchanged moves none
// of its own records. It does move the service: while the others are still
// restored the replica read is the only fresh one, and once they are read too
// the service is back to all of them. Each step is published, and owed a serial
// step, since the stored serial does not describe it.
func TestARestoredReplicaReadUnchangedTakesItsPlace(t *testing.T) {
	t.Parallel()

	const (
		service = "frontend.shop.example."
		zone    = "shop.example."
		healthy = shared.HealthStatusHealthy
		updated = incusapi.EventLifecycleInstanceUpdated
	)

	p := New(slog.Default(), Suffix("example"))
	p.next = func(_ *iutil.Event) {}

	p.fold(restoredReplica("frontend-1", "10.0.0.5", healthy))
	p.fold(restoredReplica("frontend-2", "10.0.0.6", healthy))
	require.True(t, p.state.write(0))
	p.state.step(false, 0)

	assert.Equal(t, []string{"10.0.0.5", "10.0.0.6"}, answered(t, p.state.snapshot(), service, "10.0.0.5"))
	assert.False(t, p.state.serials[zone].Owed, "the replay owes a step")

	p.fold(replica(updated, "frontend-1", "10.0.0.5", true, healthy))
	assert.True(t, p.state.write(0), "the service moved and nothing was published")
	assert.Equal(t, []string{"10.0.0.5"}, answered(t, p.state.snapshot(), service, "10.0.0.5"),
		"a restored replica answered beside one read in this run")
	assert.True(t, p.state.serials[zone].Owed, "a change the stored serial does not describe")
	p.state.step(false, 0)

	p.fold(replica(updated, "frontend-2", "10.0.0.6", true, healthy))
	assert.True(t, p.state.write(0))
	assert.Equal(t, []string{"10.0.0.5", "10.0.0.6"}, answered(t, p.state.snapshot(), service, "10.0.0.5"))

	// Read again, unchanged and fresh both times: nothing to publish.
	p.fold(replica(updated, "frontend-2", "10.0.0.6", true, healthy))
	assert.False(t, p.state.write(0), "a read that changed nothing was published")
}

// TestAConfirmedReplicaWithoutAServicePublishesNothing: a restored instance
// read unchanged that carries no service name moves nothing at all.
func TestAConfirmedReplicaWithoutAServicePublishesNothing(t *testing.T) {
	t.Parallel()

	p := New(slog.Default(), Suffix("example"))
	p.next = func(_ *iutil.Event) {}

	web := func() *iutil.Event {
		return eventOn(incusapi.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"net-a": "10.0.0.5"})
	}

	p.fold(web().WithRestored())
	require.True(t, p.state.write(0))

	p.fold(web())
	assert.False(t, p.state.write(0))
}
