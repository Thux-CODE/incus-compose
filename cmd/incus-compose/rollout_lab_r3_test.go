package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

// The health-check marker goes on the victim before the last round of reads,
// so that the stop fits in its freshness; it is set once, and is still on
// during that round.
func TestRolloutStoppingBeforeTheLastRound(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	plan := f.plan(1)

	markedDuringLastRound := false

	f.afterSleep = func(d time.Duration) {
		if d == plan.Grace {
			f.onHealth = func(string) {
				if f.replicas[1] != nil && f.replicas[1].stopping {
					markedDuringLastRound = true
				}
			}
		}
	}

	err := rollout(t.Context(), f, plan)
	require.NoError(t, err)

	assert.True(t, markedDuringLastRound)
	assert.Equal(t, []string{"web-1 true"}, f.stoppings)
}

// Every way back takes the marker off: the undrain does it for a drain of the
// rollout, the marker alone goes for a drain by hand.
func TestRolloutStoppingTakenOffOnTheWayBack(t *testing.T) {
	t.Parallel()

	for _, byHand := range []bool{false, true} {
		name := "drained by the rollout"
		if byHand {
			name = "drained by hand"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := old()
			r.drained = byHand

			f := newFakeRollout(t, map[int]*fakeReplica{1: r}, slowStart)
			f.minReady = 0
			plan := f.plan(1)

			f.afterSleep = func(d time.Duration) {
				if d == plan.Grace {
					f.replicas[2].seen = shared.HealthStatusUnhealthy
					f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
				}
			}

			err := rollout(t.Context(), f, plan)
			require.ErrorIs(t, err, errRolloutShort)

			require.NotNil(t, f.replicas[1])
			assert.False(t, f.replicas[1].stopping, "stoppings=%v events=%v", f.stoppings, f.events)
			assert.Equal(t, byHand, f.replicas[1].drained)
		})
	}
}

// D2: a new replica that turned unhealthy has its health checking stopped
// right at the verdict, before the reads and the take-down, so that
// ic-healthd's restart policy does not hold it busy as it is deleted.
func TestRolloutStoppingAtTheVerdict(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, []string{shared.HealthStatusStarting, shared.HealthStatusUnhealthy})

	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutUnhealthy)

	assert.Equal(t, []string{"web-2 true"}, f.stoppings)
	assert.Equal(t, []string{"create web-2", "discard web-2"}, f.events)
}

// D3: two stopped replicas and one declared. The one that carries a run's
// mark is a new replica an earlier run left failed: it goes first, as failed
// new ones do, and the old one goes once the new one serves. The dry run says
// the one create the run takes.
func TestRolloutFailedNewOfAnEarlierRunGoesFirst(t *testing.T) {
	t.Parallel()

	failed := &fakeReplica{runID: "failed", statuses: []string{shared.HealthStatusUnhealthy}}
	stoppedOld := &fakeReplica{statuses: []string{shared.HealthStatusStopped}}

	f := newFakeRollout(t, map[int]*fakeReplica{1: failed, 2: stoppedOld}, slowStart)
	plan := f.plan(1)

	var dry bytes.Buffer

	described := plan
	described.Out = &dry
	rolloutDescribe(described)
	assert.Contains(t, dry.String(), "; 1 create, ")
	assert.Contains(t, dry.String(), "web-1: stopped, left by an earlier run: removed first\n")
	assert.NotContains(t, dry.String(), "web-2: stopped, left by an earlier run")

	err := rollout(t.Context(), f, plan)
	require.NoError(t, err, "events=%v", f.events)

	assert.Equal(t, []string{"retire web-1", "create web-1", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 new"}, f.final())
	assert.Equal(t, 1, f.creates)
}
