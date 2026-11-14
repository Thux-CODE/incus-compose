package http

import (
	"errors"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// send hands one event to Handle, stamped the way the source would.
func send(p *Plugin, action string, chain iutil.ChainState) {
	p.Handle(iutil.NewEvent(time.Now(), action, "", "", "").WithChainState(chain))
}

// ready asks /ready and returns the status it answered.
func ready(t *testing.T, p *Plugin) int {
	t.Helper()

	w := httptest.NewRecorder()
	p.serveReady(w, httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil))

	return w.Code
}

// TestReadinessLatchesRatherThanLevels checks readiness latches on the chain state instead of tracking it as a level.
func TestReadinessLatchesRatherThanLevels(t *testing.T) {
	t.Parallel()

	p := New(slog.Default())
	p.next = func(_ *iutil.Event) {}

	send(p, iutil.ActionConnected, iutil.ChainCold)
	assert.Equal(t, 503, ready(t, p), "connected is not published")

	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p), "the first round is what there is to serve")

	// A round still running carries no chain-state change at all.
	send(p, incusapi.EventLifecycleInstanceUpdated, iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p), "a round is not an outage")

	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p), "and it is still ready after one")

	// The stream going is the one thing that does clear readiness.
	send(p, iutil.ActionDisconnected, iutil.ChainCold)
	assert.Equal(t, 503, ready(t, p), "a lost stream is unready")
}

// TestReadinessNamesWhichHalfIsMissing distinguishes a disconnected stream from a chain that has read nothing.
func TestReadinessNamesWhichHalfIsMissing(t *testing.T) {
	t.Parallel()

	p := New(slog.Default())
	p.next = func(_ *iutil.Event) {}

	w := httptest.NewRecorder()
	p.serveReady(w, httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil))
	assert.Contains(t, w.Body.String(), "not connected")

	send(p, iutil.ActionConnected, iutil.ChainCold)

	w = httptest.NewRecorder()
	p.serveReady(w, httptest.NewRequestWithContext(t.Context(), "GET", "/ready", nil))
	assert.Contains(t, w.Body.String(), "nothing published yet")
}

// TestADroppedWarmEventIsNotReady pins that readiness follows what was folded,
// not what was stamped: an end of round dropped before the plugin that serves
// it says nothing about what is served, and the next warm event that was not
// dropped is what does.
func TestADroppedWarmEventIsNotReady(t *testing.T) {
	t.Parallel()

	p := New(slog.Default())
	p.next = func(_ *iutil.Event) {}

	send(p, iutil.ActionConnected, iutil.ChainCold)

	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionSweepEnd, "", "", "").
		WithChainState(iutil.ChainWarm).WithDropped("dns"))
	assert.Equal(t, 503, ready(t, p), "a dropped end of round is not one")

	send(p, "dns/round-lost", iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p), "the next one that was folded is")
}

// TestReadinessOutlivesAColdEvent pins that once a round has ended, only the
// stream going takes readiness back. A read that failed, one dropped, and one
// minted cold before the round ended and landing after it all walk past here
// stamped cold, and none of them is what is served going anywhere.
func TestReadinessOutlivesAColdEvent(t *testing.T) {
	t.Parallel()

	p := New(slog.Default())
	p.next = func(_ *iutil.Event) {}

	send(p, iutil.ActionConnected, iutil.ChainCold)
	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)
	require.Equal(t, 200, ready(t, p))

	late := iutil.NewEvent(time.Now(), incusapi.EventLifecycleInstanceUpdated, "pricelist", "django-1", "").
		WithChainState(iutil.ChainCold)

	p.Handle(late.WithFailed(errors.New("enricher/read")))
	assert.Equal(t, 200, ready(t, p), "a failed read is not an outage")

	p.Handle(late)
	assert.Equal(t, 200, ready(t, p), "a read minted cold and landing after the round is not one either")

	p.Handle(late.WithDropped("dns"))
	assert.Equal(t, 200, ready(t, p), "nor one dropped on the way")

	send(p, iutil.ActionDisconnected, iutil.ChainCold)
	assert.Equal(t, 503, ready(t, p), "a lost stream is unready")

	// Back, but nothing re-read yet: the stream is up and readiness is not.
	send(p, iutil.ActionConnected, iutil.ChainCold)
	assert.Equal(t, 503, ready(t, p), "a reconnect is not a round")

	// A warm event stamped before the stream went is a leftover, and says
	// nothing about the stream since.
	p.Handle(iutil.NewEvent(time.Now().Add(-time.Hour), incusapi.EventLifecycleInstanceUpdated, "shop", "web", "").
		WithChainState(iutil.ChainWarm))
	assert.Equal(t, 503, ready(t, p), "a warm leftover from before the stream went")

	send(p, iutil.ActionSweepEnd, iutil.ChainWarm)
	assert.Equal(t, 200, ready(t, p), "the round ended again")
}
