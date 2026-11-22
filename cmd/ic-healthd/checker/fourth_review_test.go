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

func TestFourthStoppedReadAfterCompletedRestart(t *testing.T) {
	s := newSchedulerWithPools(t, 1, 1)
	free := make(chan struct{})
	require.NoError(t, s.pools.check.Submit(func() { <-free }))
	require.NoError(t, s.pools.restart.Submit(func() { <-free }))
	defer close(free)
	inst := s.add("web", testConfig())
	inst.action = instanceActionRestart
	restart := s.restarted(t, inst, nil)
	s.event(api.EventLifecycleInstanceStarted, "web")
	s.event(api.EventLifecycleInstanceStopped, "web")
	s.run()
	s.result(restart)
	s.run()
	s.eventRead(api.EventLifecycleInstanceUpdated, "web", false, healthKeys(eventConfig(inst.config)))
	s.eventRead(api.EventLifecycleInstanceUpdated, "web", false, healthKeys(eventConfig(inst.config)))
	wake := s.run()
	require.False(t, wake.IsZero(), "a stopped instance with an active policy has no scheduled work, state=%v action=%q", inst.state, inst.action)
	require.Equal(t, instanceActionRestart, inst.action)
}
