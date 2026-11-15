package dns

import (
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"
)

// drained is a replica with the drain label on, in the shape replica builds.
func drained(name, addr, status string) *iutil.Event {
	config := map[string]string{
		userLabel(labelServiceCompose):     "frontend",
		userLabel(labelPrefix + metaDrain): "true",
		shared.HealthEnabledKey:            "true",
		shared.HealthKeyPrefix + "test":    healthTest,
		shared.HealthStatusKey:             status,
	}

	return event(incusapi.EventLifecycleInstanceStarted, "shop", name).
		WithInstance(read("shop", true, config, netsOf(map[string]string{"net-a": addr})), true)
}

// A drain is the operator's word and readiness does not overrule it: drained
// replicas are no candidates, ready or not, and fail-open only reaches the ones
// left.
func TestDrainBeatsFailOpen(t *testing.T) {
	t.Parallel()

	t.Run("drained and healthy, other unhealthy: only the unhealthy", func(t *testing.T) {
		t.Parallel()

		snap := fed(t,
			drained("frontend-1", "10.0.0.5", shared.HealthStatusHealthy),
			replica(incusapi.EventLifecycleInstanceStarted, "frontend-2", "10.0.0.6", true, shared.HealthStatusUnhealthy),
		).state.snapshot()

		assert.Equal(t, []string{"10.0.0.6"}, answered(t, snap, "frontend.shop.example.", "10.0.0.5"))
	})

	t.Run("every replica drained: no answer", func(t *testing.T) {
		t.Parallel()

		snap := fed(t,
			drained("frontend-1", "10.0.0.5", shared.HealthStatusHealthy),
			drained("frontend-2", "10.0.0.6", shared.HealthStatusUnhealthy),
		).state.snapshot()

		assert.Nil(t, answered(t, snap, "frontend.shop.example.", "10.0.0.5"))
		assert.Equal(t, []string{"10.0.0.5"}, answered(t, snap, "frontend-1.shop.example.", "10.0.0.5"))
	})
}
