package checker

import (
	"errors"
	"testing"

	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/shared"
)

// TestSixthStopAfterUnhealthyThenRunningRead: an unhealthy run makes a
// restart due with restartStops, which stops the instance first. Before it
// goes out the instance stops (event or read, either) - the unhealthy run is
// over - and someone starts it again with the start event lost: the read
// says running. That is a new run nobody has checked, the case N13 is about,
// but restartStops is still true: the read keeps the restart, and the worker
// force-stops the running instance. Expected: once a stop has been seen, the
// restart is one for a stop, and a running read calls it off.
func TestSixthStopAfterUnhealthyThenRunningRead(t *testing.T) {
	for _, viaEvent := range []bool{true, false} {
		name := "stop read"
		if viaEvent {
			name = "stop event"
		}

		t.Run(name, func(t *testing.T) {
			s := newScheduler(t)

			cfg := testConfig()
			cfg.retries = 1
			inst := s.add("web-1", cfg)
			inst.status = shared.HealthStatusHealthy

			s.result(s.checked(t, inst, errors.New("down")))
			require.Equal(t, instanceActionRestart, inst.action)
			require.True(t, inst.restartStops)

			if viaEvent {
				s.event(incusApi.EventLifecycleInstanceStopped, "web-1")
			} else {
				s.stoppedRead(inst, false)
			}

			require.False(t, inst.config.running)

			s.event(incusApi.EventLifecycleInstanceUpdated, "web-1")
			require.True(t, inst.config.running)

			require.False(t, inst.action == instanceActionRestart && inst.restartStops,
				"a run started after the stop, unseen, keeps a restart that force-stops it: action=%v restartStops=%v", inst.action, inst.restartStops)
		})
	}
}
