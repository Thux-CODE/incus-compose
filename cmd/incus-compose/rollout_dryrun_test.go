package main

import (
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/shared"
)

// A healthcheck a profile alone gives the replicas is one ic-healthd runs: the
// service is not refused for having none.
func TestRolloutProfileHealth(t *testing.T) {
	t.Parallel()

	old := &api.Instance{
		InstancePut: api.InstancePut{Config: map[string]string{shared.HealthStatusKey: shared.HealthStatusHealthy}},
		ExpandedConfig: map[string]string{
			shared.HealthStatusKey:          shared.HealthStatusHealthy,
			shared.HealthEnabledKey:         "true",
			shared.HealthKeyPrefix + "test": `["CMD","true"]`,
			"user.other":                    "x",
		},
	}

	assert.False(t, rolloutWatched(map[string]string{}))

	health := rolloutProfileHealth(map[string]string{}, old)
	assert.True(t, rolloutWatched(health))
	assert.NotContains(t, health, "user.other")
	assert.NotContains(t, health, shared.HealthStatusKey, "the status is the replica's own")

	// The compose file's own test wins.
	health = rolloutProfileHealth(map[string]string{shared.HealthKeyPrefix + "test": `["NONE"]`}, old)
	assert.Equal(t, `["NONE"]`, health[shared.HealthKeyPrefix+"test"])
}

// A dry run with the image not built keeps the refusal of a service with no
// healthcheck when the compose file turns it off, by disable or by NONE: the
// image's check is not taken then either.
func TestRolloutHealthDisabled(t *testing.T) {
	t.Parallel()

	assert.False(t, rolloutHealthDisabled(types.ServiceConfig{}))
	assert.False(t, rolloutHealthDisabled(types.ServiceConfig{HealthCheck: &types.HealthCheckConfig{Test: types.HealthCheckTest{"CMD", "true"}}}))
	assert.True(t, rolloutHealthDisabled(types.ServiceConfig{HealthCheck: &types.HealthCheckConfig{Disable: true}}))
	assert.True(t, rolloutHealthDisabled(types.ServiceConfig{HealthCheck: &types.HealthCheckConfig{Test: types.HealthCheckTest{"NONE"}}}))
}
