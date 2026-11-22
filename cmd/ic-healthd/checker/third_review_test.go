package checker

import (
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

func TestThirdNewRunHealthyQueuedBehindOldHealthy(t *testing.T) {
	s := newScheduler(t)
	inst := s.add("web", testConfig())
	inst.status = shared.HealthStatusUnhealthy
	s.result(s.checked(t, inst, nil))
	old := s.next(t)
	s.event(api.EventLifecycleInstanceRestarted, "web")
	s.result(s.checked(t, inst, nil))
	require.Equal(t, shared.HealthStatusHealthy, inst.queued)
	s.result(landed(old))
	require.True(t, inst.unproven)
	require.True(t, inst.writing, "the new run's queued healthy must be sent")
}
