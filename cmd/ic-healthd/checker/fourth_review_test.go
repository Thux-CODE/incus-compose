package checker

import (
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

func TestFourthHealthyQueuedBeforeStart(t *testing.T) {
	s := newScheduler(t)
	cfg := testConfig()
	cfg.test = []string{"NONE"}
	inst := s.add("web", cfg)
	s.event(api.EventLifecycleInstanceStarted, "web")
	s.result(s.checked(t, inst, nil))
	first := s.next(t)
	s.result(s.checked(t, inst, nil))
	require.Equal(t, shared.HealthStatusHealthy, inst.queued)
	s.event(api.EventLifecycleInstanceRestarted, "web")
	s.result(landed(first))
	if inst.writing {
		s.result(landed(s.next(t)))
	}
	require.True(t, inst.unproven, "there has been no check in the new run")
}
