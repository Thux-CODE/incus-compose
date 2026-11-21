package main

import (
	"context"
	"testing"
	"time"

	incusApi "github.com/lxc/incus/v7/shared/api"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

// The case met on veriax-demo: the graceful stop returns with the victim still
// running (a passing Error read), two reads in a row. The first try reads it
// running and asks for a new round; on that round's try the kill goes out, and
// the stop ends once the victim reads Stopped.
func TestRolloutStopKillsAfterAFreshRound(t *testing.T) {
	t.Parallel()

	try := func(killAfterStop bool) (int, error) {
		kills := 0
		reads := 0

		shutdown := func(context.Context) error { return nil }
		kill := func(context.Context) error {
			kills++
			return nil
		}
		status := func(context.Context) (incusApi.StatusCode, error) {
			reads++
			if kills > 0 {
				return incusApi.Stopped, nil
			}

			return incusApi.Running, nil
		}
		afterStop := func() error {
			if !killAfterStop {
				return errRolloutRestarted
			}

			return nil
		}

		err := rolloutStopAfter(t.Context(), shutdown, kill, status, true, time.Second, time.Millisecond, time.Millisecond, afterStop)

		return kills, err
	}

	// The first try: a new round, no kill.
	kills, err := try(false)
	require.ErrorIs(t, err, errRolloutRestarted)
	assert.Zero(t, kills)

	// The try after it: one kill, and the stop goes through.
	kills, err = try(true)
	require.NoError(t, err)
	assert.Equal(t, 1, kills)
}

// A stop that would begin past the freshness of the round it goes by, or a
// victim ic-healthd started again after its stop, takes a new round first:
// the stop goes by what that round reads.
func TestRolloutStopGoesByAFreshRound(t *testing.T) {
	t.Parallel()

	for _, put := range []error{errRolloutStale, errRolloutRestarted} {
		t.Run(put.Error(), func(t *testing.T) {
			t.Parallel()

			f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
			f.retireErrs = []error{put}

			err := rollout(t.Context(), f, f.plan(1))
			require.NoError(t, err)

			assert.Equal(t, []string{"create web-2", "drain web-1", "retire web-1 put off", "retire web-1"}, f.events)
			assert.Equal(t, []string{"web-2 new"}, f.final())
		})
	}

	t.Run("a victim started again is killed on the next try, its round fresh", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
		f.retireErrs = []error{errRolloutRestarted}

		err := rollout(t.Context(), f, f.plan(1))
		require.NoError(t, err)

		assert.Equal(t, []bool{false, true}, f.killAfterStop, "the try after a restart may kill")
		assert.Equal(t, []string{"create web-2", "drain web-1", "retire web-1 put off", "retire web-1"}, f.events)
	})

	t.Run("and the new round may say no", func(t *testing.T) {
		t.Parallel()

		f := newFakeRollout(t, map[int]*fakeReplica{1: old()}, slowStart)
		f.minReady = 0
		f.retireErrs = []error{errRolloutStale}

		f.onRetire = func() {
			f.replicas[2].seen = shared.HealthStatusUnhealthy
			f.replicas[2].statuses = []string{shared.HealthStatusUnhealthy}
		}

		err := rollout(t.Context(), f, f.plan(1))
		require.ErrorIs(t, err, errRolloutShort)

		assert.Equal(t, []string{"create web-2", "drain web-1", "retire web-1 put off", "undrain web-1", "discard web-2"}, f.events)
		assert.Equal(t, []string{"web-1 old"}, f.final())
	})
}
