package checker

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/iclient"
	"github.com/lxc/incus-compose/shared"
)

// stateConn answers every request with an instance state carrying code, which
// is all a check with nothing to exec asks of Incus.
func stateConn(t *testing.T, code incusApi.StatusCode) *iclient.Connection {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"sync","status_code":200,"metadata":{"status":"`+
			code.String()+`","status_code":`+strconv.Itoa(int(code))+`}}`)
	}))
	t.Cleanup(server.Close)

	conn, err := iclient.NewConnection(&iclient.ConfigRemoteInfo{Name: "state", Addrs: []string{server.URL}})
	require.NoError(t, err)

	return conn
}

// TestInstanceRestartActionLeavesARunningInstanceForAStop: a restart owed for a
// stop that finds the instance running - started since by someone else - sends
// no stop and no start. One owed for an unhealthy run still stops it first.
func TestInstanceRestartActionLeavesARunningInstanceForAStop(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		code       incusApi.StatusCode
		stops      bool
		wantNoop   bool
		wantChange bool
		wantError  bool
	}{
		{name: "running, for a stop", code: incusApi.Running, wantNoop: true},
		{name: "ready, for a stop", code: incusApi.Ready, wantNoop: true},
		{name: "stopped, for a stop", code: incusApi.Stopped, wantChange: true, wantError: true},
		{name: "running, unhealthy", code: incusApi.Running, stops: true, wantChange: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var changes atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				// A state change opens the event socket before its PUT.
				if r.Method == http.MethodPut || strings.HasSuffix(r.URL.Path, "/events") {
					changes.Add(1)
					_, _ = io.WriteString(w, `{"type":"error","error_code":500,"error":"refused by the test"}`)

					return
				}

				_, _ = io.WriteString(w, `{"type":"sync","status_code":200,"metadata":{"status":"`+
					tt.code.String()+`","status_code":`+strconv.Itoa(int(tt.code))+`}}`)
			}))
			t.Cleanup(server.Close)

			conn, err := iclient.NewConnection(&iclient.ConfigRemoteInfo{Name: "state", Addrs: []string{server.URL}})
			require.NoError(t, err)

			res := instanceRestartAction(t.Context(), conn, "p", "web-1", tt.stops)

			require.Equal(t, tt.wantNoop, res.alreadyRunning)
			require.Equal(t, tt.wantChange, changes.Load() > 0, "state changes sent: %d", changes.Load())

			if tt.wantError {
				require.Error(t, res.err)
			} else {
				require.NoError(t, res.err)
			}
		})
	}
}

// TestInstanceCheckActionReadyIsRunning: Ready is a running instance whose
// guest said so through /dev/incus. Taken for a stop, the scheduler would
// write stopped and, with a restart policy, force-stop a working container.
func TestInstanceCheckActionReadyIsRunning(t *testing.T) {
	t.Parallel()

	for code, running := range map[incusApi.StatusCode]bool{
		incusApi.Running:  true,
		incusApi.Ready:    true,
		incusApi.Stopped:  false,
		incusApi.Frozen:   false,
		incusApi.Freezing: false,
		incusApi.Starting: false,
		incusApi.Stopping: false,
		incusApi.Error:    false,
	} {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()

			res := instanceCheckAction(t.Context(), slog.Default(), stateConn(t, code), "p", "web-1",
				&instanceConfig{test: []string{"NONE"}, timeout: time.Second})

			if running {
				require.NoError(t, res.err)
			} else {
				require.ErrorIs(t, res.err, ErrNotRunning)
			}
		})
	}
}

// TestAReadyInstanceStaysWatchedAndHealthy is the linear case through the
// scheduler: a Ready instance's check passes, healthy is written, and neither
// stopped nor a restart follows.
func TestAReadyInstanceStaysWatchedAndHealthy(t *testing.T) {
	t.Parallel()

	s := newScheduler(t)

	cfg := testConfig()
	cfg.test = []string{"NONE"}
	inst := s.add("web-1", cfg)

	res := s.checked(t, inst, nil)
	res.err = instanceCheckAction(t.Context(), slog.Default(), stateConn(t, incusApi.Ready), s.project, "web-1", cfg).err
	s.result(res)

	written := s.next(t)
	require.Equal(t, shared.HealthStatusHealthy, written.status)
	require.True(t, inst.config.running)
	require.Equal(t, instanceActionCheck, inst.action, "no restart for a running instance")
}
