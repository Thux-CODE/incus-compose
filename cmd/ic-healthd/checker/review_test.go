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
