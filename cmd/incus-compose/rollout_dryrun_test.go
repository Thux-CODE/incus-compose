package main

import (
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/shared"
)

// A healthcheck a profile alone gives the replicas is one ic-healthd runs: the
// service is not refused for having none.
func TestRolloutProfileHealth(t *testing.T) {
	t.Parallel()

	probe := map[string]string{
		shared.HealthEnabledKey:         "true",
		shared.HealthKeyPrefix + "test": `["CMD","true"]`,
		"user.other":                    "x",
	}

	assert.False(t, rolloutWatched(map[string]string{}))

	health := rolloutProfileHealth(map[string]string{}, []map[string]string{{}, probe})
	assert.True(t, rolloutWatched(health))
	assert.NotContains(t, health, "user.other")

	// A later profile wins over an earlier one, the compose file over both.
	later := map[string]string{shared.HealthKeyPrefix + "test": `["CMD","later"]`}
	health = rolloutProfileHealth(map[string]string{}, []map[string]string{probe, later})
	assert.Equal(t, `["CMD","later"]`, health[shared.HealthKeyPrefix+"test"])

	health = rolloutProfileHealth(map[string]string{shared.HealthKeyPrefix + "test": `["NONE"]`}, []map[string]string{probe})
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
