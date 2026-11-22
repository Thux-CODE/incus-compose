package checker

import (
	"context"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

func TestThirdUnwatchedWithAlreadyFullResults(t *testing.T) {
	s := newScheduler(t)
	for range cap(s.results) {
		s.results <- instanceResult{}
	}
	ctx, cancel := context.WithCancel(t.Context())
	s.ctx = ctx
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.eventRead(api.EventLifecycleInstanceUpdated, "web", true, healthKeys(map[string]string{"test": "invalid JSON"}))
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-done:
	case <-time.After(absenceGrace):
		t.Fatal("the event handler waited for space in results")
	}
	require.Len(t, s.results, cap(s.results))
}

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
