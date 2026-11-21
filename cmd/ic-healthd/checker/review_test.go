package checker

import (
	"github.com/lxc/incus-compose/shared"
	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReviewPreviousRunCannotMarkNewRunHealthy(t *testing.T) {
	s := newScheduler(t)
	cfg := testConfig()
	cfg.startPeriod = time.Minute
	inst := s.add("web", cfg)
	previous := s.checked(t, inst, nil)
	s.event(incusapi.EventLifecycleInstanceRestarted, "web")
	starting := s.next(t)
	require.Equal(t, shared.HealthStatusStarting, starting.status)
	s.result(landed(starting))
	s.result(previous)
	require.Equal(t, shared.HealthStatusStarting, inst.status)
}

func TestReviewStatusReadWhileStartingInFlight(t *testing.T) {
	s := newScheduler(t)
	cfg := testConfig()
	cfg.startPeriod = time.Minute
	inst := s.add("web", cfg)
	keys := eventConfig(cfg)
	keys["status"] = shared.HealthStatusHealthy
	s.eventRead(incusapi.EventLifecycleInstanceStarted, "web", true, healthKeys(keys))
	starting := s.next(t)
	require.Equal(t, shared.HealthStatusStarting, starting.status)
	// An update read the old config before the starting write landed.
	s.eventRead(incusapi.EventLifecycleInstanceUpdated, "web", true, healthKeys(keys))
	s.result(landed(starting))
	s.result(s.checked(t, inst, nil))
	select {
	case result := <-s.results:
		require.Equal(t, shared.HealthStatusHealthy, result.status)
	case <-time.After(absenceGrace):
		t.Fatal("starting landed; first successful check requested no healthy write")
	}
}
