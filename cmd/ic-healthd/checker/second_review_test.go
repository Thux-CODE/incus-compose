package checker

import (
	"testing"
	"time"

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
