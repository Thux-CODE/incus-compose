package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

// Regression tests from an adversarial review of rollout: each one is a
// sequence that left the service with fewer healthy replicas than declared,
// or retired a replica on a read that could not tell.

func reviewCurrent() *fakeReplica {
	r := old()
	r.fresh, r.current = true, true
	return r
}

// Picked up again with the new replica unhealthy and the old one drained, the
// old one is the only one that serves: it is put back and the new one given
// back. That is a rollback, and the run ends with an error that says so. The
// review asked for the drain to be kept, which leaves nothing serving; the put
// back stays, and the error is what makes it visible.
func TestReviewDrainedOnResume(t *testing.T) {
	oldOne := old()
	oldOne.drained = true
	newOne := reviewCurrent()
	newOne.statuses = []string{shared.HealthStatusUnhealthy}
	f := newFakeRollout(t, map[int]*fakeReplica{1: oldOne, 2: newOne})
	plan := f.plan(1)

	err := rollout(t.Context(), f, plan)
	require.ErrorIs(t, err, errRolloutUnhealthy)

	assert.Equal(t, []string{"undrain web-1", "drain web-2", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
	assert.Contains(t, plan.Out.(*bytes.Buffer).String(), "web-1: drained by an earlier rollout, put back: without it 0 of 1 declared would serve")
}

// Three failed reads of a replica an earlier run made are no verdict: the run
// stops and retires nothing, where it used to take the replica for old and
// retire it on the index.
func TestReviewResumeReadFailure(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: reviewCurrent(), 2: old()})
	f.minReady = 1
	f.healthErrs = rolloutHealthReads

	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutUnread)

	require.NotNil(t, f.replicas[1])
	require.NotNil(t, f.replicas[2])
	assert.Empty(t, f.events)
}

// With two replicas, an old one that turns unhealthy while the new one gets
// healthy is the one to go: the old healthy one and the new one serve.
func TestReviewHealthyBudget(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: old()}, slowStart, slowStart)
	f.onHealth = func(s string) {
		if s == shared.HealthStatusHealthy {
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
			f.onHealth = nil
		}
	}

	err := rollout(t.Context(), f, f.plan(2))
	require.NoError(t, err)

	assert.Equal(t, []string{
		"create web-3", "drain web-2", "retire web-2",
		"create web-2", "drain web-1", "retire web-1",
	}, f.events)
}

// The new replica restarted during the grace of the old one's drain: the
// stop is put off, the old one put back, and the run fails.
func TestReviewRestartDuringGrace(t *testing.T) {
	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	plan := f.plan(1)
	f.onSleep = func(d time.Duration) {
		if d == plan.Grace {
			f.replicas[2].seen = shared.HealthStatusStarting
			f.replicas[2].statuses = []string{shared.HealthStatusStarting}
		}
	}

	err := rollout(t.Context(), f, plan)
	require.ErrorIs(t, err, errRolloutShort)
	assert.Contains(t, err.Error(), "web-1 not retired")

	assert.Equal(t, []string{"create web-2", "drain web-1", "undrain web-1"}, f.events)
	assert.Equal(t, 1, f.ready())
}

// An up to date replica that reads healthy, then unhealthy as the surplus is
// read, is old again: it is given back, the old one serves on, and the run
// fails.
func TestReviewSurplusAfterHealthChange(t *testing.T) {
	r := reviewCurrent()
	r.statuses = []string{shared.HealthStatusHealthy, shared.HealthStatusUnhealthy}
	f := newFakeRollout(t, map[int]*fakeReplica{1: old(), 2: r})
	f.minReady = 1

	err := rollout(t.Context(), f, f.plan(1))
	require.ErrorIs(t, err, errRolloutLapsed)

	assert.Equal(t, []string{"drain web-2", "retire web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
}

// An interrupt during the grace of a replica an earlier run drained leaves
// it drained: the drain is not this run's to take off.
func TestReviewKeepsAnEarlierDrain(t *testing.T) {
	oldOne := old()
	oldOne.drained = true
	f := newFakeRollout(t, map[int]*fakeReplica{1: oldOne, 2: reviewCurrent()})
	plan := f.plan(1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	f.onSleep = func(d time.Duration) {
		if d == plan.Grace {
			cancel()
		}
	}

	err := rollout(ctx, f, plan)
	require.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, []string{"drain web-1"}, f.events)
	assert.Equal(t, []string{"web-1 old drained", "web-2 new"}, f.final())
}
