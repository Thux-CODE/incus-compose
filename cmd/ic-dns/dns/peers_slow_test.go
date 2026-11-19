package dns

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestASlowReadIsNotCutShort pins the deadline per call: every call slower
// than a whole read used to be allowed, all of them inside their own, and the
// read lands however long it takes in total.
func TestASlowReadIsNotCutShort(t *testing.T) {
	t.Parallel()

	f := hubIncus()
	f.delay = 30 * time.Millisecond

	start := time.Now()

	got, err := readPeers(t.Context(), bounded{r: f, timeout: 50 * time.Millisecond}, hubKeys())
	require.NoError(t, err)
	assert.Equal(t, hubPeers(), got)
	assert.Greater(t, time.Since(start), 50*time.Millisecond, "the read as a whole outlasted one call's deadline")
}

// TestACallPastItsDeadlineFailsTheRead pins the other half: one call that does
// not answer within its own deadline fails the read, so what is held stays.
func TestACallPastItsDeadlineFailsTheRead(t *testing.T) {
	t.Parallel()

	f := hubIncus()
	f.delay = 80 * time.Millisecond

	_, err := readPeers(t.Context(), bounded{r: f, timeout: 40 * time.Millisecond}, hubKeys())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestPluginReadsThroughASlowIncus is the restart on a slow member: each call
// within its deadline, the whole read longer than any one of them, and the
// replica warms once it lands.
func TestPluginReadsThroughASlowIncus(t *testing.T) {
	f := hubIncus()
	f.delay = 25 * time.Millisecond

	r := runPeeredWith(t, f, func(p *Plugin) { p.peerCallTimeout = 60 * time.Millisecond })
	r.feed(t, append(hubFleetEvents(false), event(iutil.ActionSweepEnd, "", ""))...)

	require.Eventually(t, func() bool { return r.p.PeeringReady() == nil }, 3*time.Second, 10*time.Millisecond)

	r.stop(t)
}
