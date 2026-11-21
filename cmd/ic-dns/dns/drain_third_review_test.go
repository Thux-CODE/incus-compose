package dns

import (
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

func TestThirdReviewDNSSpacedDrain(t *testing.T) {
	cfg := map[string]string{
		userLabel(labelServiceCompose):     "frontend",
		userLabel(labelPrefix + metaDrain): " true ",
		shared.HealthEnabledKey:            "true", shared.HealthKeyPrefix + "test": healthTest,
		shared.HealthStatusKey: shared.HealthStatusHealthy,
	}
	e := event(api.EventLifecycleInstanceStarted, "shop", "frontend-2").WithInstance(read("shop", true, cfg, netsOf(map[string]string{"net-a": "10.0.0.6"})), true)
	snap := fed(t, e).state.snapshot()
	require.Empty(t, answered(t, snap, "frontend.shop.example.", "10.0.0.6"))
	require.Equal(t, []string{"10.0.0.6"}, answered(t, snap, "frontend-2.shop.example.", "10.0.0.6"))
}
