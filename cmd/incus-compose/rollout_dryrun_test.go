package main

import (
	"testing"

	"github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
)

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
