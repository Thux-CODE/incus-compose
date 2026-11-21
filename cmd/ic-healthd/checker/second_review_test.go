package checker

import (
	"context"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/shared"
)

func TestSecondWatchdogExpiresStartPeriod(t *testing.T) {
	s := newScheduler(t)
	cfg := testConfig()
	cfg.retries = 1
	cfg.restart = "always"
	cfg.startPeriod = time.Second
	inst := s.add("web", cfg)
	instanceStarted(inst, time.Now().Add(-time.Minute))
	inst.status = shared.HealthStatusStarting
	for range 3 {
		s.running(t, inst, instanceChecking, time.Now().Add(-time.Second))
		s.run()
		if inst.action == instanceActionRestart {
			return
		}
	}
	t.Fatal("three timed out probes after the start period, still starting without restart")
}

func TestSecondFullResultsBlocksScheduler(t *testing.T) {
	s := newScheduler(t)
	keys := healthKeys(map[string]string{"test": "invalid JSON"})
	for range cap(s.results) {
		s.eventRead(api.EventLifecycleInstanceUpdated, "web", true, keys)
	}
	ctx, cancel := context.WithCancel(t.Context())
	s.ctx = ctx
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.eventRead(api.EventLifecycleInstanceUpdated, "web", true, keys)
	}()
	defer func() { cancel(); <-returned }()
	select {
	case <-returned:
	case <-time.After(absenceGrace):
		t.Fatal("the results channel can be drained only after the event handler returns")
	}
}
