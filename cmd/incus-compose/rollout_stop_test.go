package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

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
