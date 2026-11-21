package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

// The case met in the lab: the new replica turns unhealthy early in a long
// grace. The old one is put back at the first round read during the grace,
// not at its end, and the new one taken away.
func TestRolloutGraceWatchesTheOthers(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
	f.minReady = 0

	plan := f.plan(1)
	plan.Grace = 90 * time.Second
	plan.GraceStep = 5 * time.Second

	var drainedAt time.Time

	f.onSleep = func(d time.Duration) {
		if d == plan.GraceStep && drainedAt.IsZero() {
			drainedAt = f.now
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
		}
	}

	err := rollout(t.Context(), f, plan)
	require.ErrorIs(t, err, errRolloutShort)
	assert.Contains(t, err.Error(), "during its grace")

	assert.Equal(t, []string{"create web-2", "drain web-1", "undrain web-1", "discard web-2"}, f.events)
	assert.Equal(t, []string{"web-1 old"}, f.final())
	assert.Equal(t, plan.GraceStep, f.now.Sub(drainedAt), "put back after one step of the grace, not after all of it")
}

// A round that cannot be read during the grace neither rolls back nor
// authorizes the stop: the grace goes on, and the check before the stop
// decides.
func TestRolloutGraceUnreadRoundGoesOn(t *testing.T) {
	t.Parallel()

	f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)

	plan := f.plan(1)
	plan.Grace = 20 * time.Second
	plan.GraceStep = 5 * time.Second

	inGrace := false
	failures := 0

	f.onSleep = func(d time.Duration) {
		if d == plan.GraceStep && !inGrace {
			inGrace = true
			failures = rolloutScanAttempts
		}
	}

	ops := secondReviewReads{f, func(_ context.Context, i int) error {
		if i == 2 && failures > 0 {
			failures--
			return io.EOF
		}

		return nil
	}}

	err := rollout(t.Context(), ops, plan)
	require.NoError(t, err)

	assert.Equal(t, []string{"create web-2", "drain web-1", "retire web-1"}, f.events)
	assert.Contains(t, plan.Out.(interface{ String() string }).String(), "could not be read during its grace")
}
