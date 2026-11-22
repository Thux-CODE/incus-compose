package checker

import (
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

func TestFifthRestartAfterMarkerClearedWhileStopped(t *testing.T) {
	s := newScheduler(t)
	keys := healthKeys(eventConfig(testConfig()))
	s.eventRead(api.EventLifecycleInstanceUpdated, "web", true, keys)
	keys[shared.HealthStoppedKey] = "true"
	s.eventRead(api.EventLifecycleInstanceUpdated, "web", false, keys)
	delete(keys, shared.HealthStoppedKey)
	s.eventRead(api.EventLifecycleInstanceUpdated, "web", false, keys)
	inst := s.instances[s.key("web")]
	require.Equal(t, instanceActionRestart, inst.action)
	require.NotEqual(t, instanceParked, inst.state)
}
