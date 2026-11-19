package http

import (
	"errors"
	"log/slog"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestAGateHoldsReadinessAfterTheRound pins the gate: a round latched is not
// enough while it says why not, its reason is what /ready answers, and once it
// says nothing the endpoint is ready without another event. The lost stream
// still comes first.
func TestAGateHoldsReadinessAfterTheRound(t *testing.T) {
	t.Parallel()

	var open atomic.Bool

	p := New(slog.Default(), Gate(func() error {
		if open.Load() {
			return nil
		}

		return errors.New("the network peering has not been read yet")
	}))
	p.next = func(_ *iutil.Event) {}

	send(p, iutil.ActionConnected, iutil.ChainCold)
	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)

	w := httptest.NewRecorder()
	p.serveReady(w, httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil))
	assert.Equal(t, 503, w.Code, "the round is not all there is to wait on")
	assert.Contains(t, w.Body.String(), "peering")

	open.Store(true)
	assert.Equal(t, 200, ready(t, p), "the gate opening is enough, no event needed")

	send(p, iutil.ActionDisconnected, iutil.ChainCold)
	assert.Equal(t, 503, ready(t, p), "an open gate does not outrank a lost stream")
}

// TestNoGateIsTheRoundAlone pins the default: without a gate nothing changes.
func TestNoGateIsTheRoundAlone(t *testing.T) {
	t.Parallel()

	p := New(slog.Default())
	p.next = func(_ *iutil.Event) {}

	send(p, iutil.ActionConnected, iutil.ChainCold)
	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p))
}
