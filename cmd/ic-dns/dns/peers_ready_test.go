package dns

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// raisedReady reports whether the plugin has raised dns/ready since the last
// look, reading what it raised without waiting.
func raisedReady(r *running) bool {
	for {
		select {
		case cmd := <-r.raised:
			if cmd.Action == "dns/ready" {
				return true
			}
		default:
			return false
		}
	}
}

// TestARoundWaitsOnTheFirstPeeringRead pins readiness with peer views on: a
// round that ends before the peering could be read leaves the view cold - a
// miss is SERVFAIL, not NXDOMAIN, PeeringReady says why and no dns/ready is
// raised - and the first read that lands turns it warm, without another
// round, raising dns/ready then.
func TestARoundWaitsOnTheFirstPeeringRead(t *testing.T) {
	const mgmt = "10.146.11.6"

	f := hubIncus()
	f.down = errors.New("incusd is not answering")

	r := runPeered(t, f, Suffix("incus"), TTL(30))

	require.ErrorIs(t, r.p.PeeringReady(), errPeeringNotRead)

	r.feed(t, append(hubFleetEvents(false), event(iutil.ActionSweepEnd, "", ""))...)

	// Its own names answer; one seen only through a peer is not there yet, and
	// says so with SERVFAIL rather than a final NXDOMAIN.
	answersWithin(t, r, "mgmt.default.incus.", mgmt, dns.RcodeSuccess, "own network, round ended")
	answersWithin(t, r, "django-1.pvlab1.incus.", mgmt, dns.RcodeServerFailure, "through a peer, not read")
	answersWithin(t, r, "nobody.default.incus.", mgmt, dns.RcodeServerFailure, "a miss is not final yet")

	assert.False(t, r.p.view.Ready(), "not warm while the peering is unread")
	assert.ErrorIs(t, r.p.PeeringReady(), errPeeringNotRead)
	assert.False(t, raisedReady(r), "dns/ready raised before the peering was read")

	// A warm event after the round changes nothing either.
	r.feed(t, warm(event(iutil.ActionSweepEnd, "", "")))
	assert.False(t, r.p.view.Ready())

	f.mu.Lock()
	f.down = nil
	f.mu.Unlock()

	answersWithin(t, r, "django-1.pvlab1.incus.", mgmt, dns.RcodeSuccess, "the read lands")
	answersWithin(t, r, "nobody.default.incus.", mgmt, dns.RcodeNameError, "and the view is warm")

	assert.NoError(t, r.p.PeeringReady())
	require.Eventually(t, func() bool { return raisedReady(r) }, 2*time.Second, 10*time.Millisecond,
		"dns/ready is raised when the read turns the view warm")

	r.stop(t)
}

// TestTheRoundTurnsWarmWhenThePeeringCameFirst pins the common order: the
// peering is read before the round ends, and the round is all it waits on.
func TestTheRoundTurnsWarmWhenThePeeringCameFirst(t *testing.T) {
	f := hubIncus()

	r := runPeered(t, f, Suffix("incus"), TTL(30))

	require.Eventually(t, func() bool { return r.p.PeeringReady() == nil }, 2*time.Second, 5*time.Millisecond)
	assert.False(t, r.p.view.Ready(), "a read is not a round")

	r.feed(t, append(hubFleetEvents(false), event(iutil.ActionSweepEnd, "", ""))...)

	answersWithin(t, r, "nobody.default.incus.", "10.146.11.6", dns.RcodeNameError, "warm at the round")
	assert.True(t, r.p.view.Ready())

	r.stop(t)
}

// TestWithoutPeerViewsNothingWaitsOnThePeering pins the flag off: ready from
// the start, and the round alone turns the view warm.
func TestWithoutPeerViewsNothingWaitsOnThePeering(t *testing.T) {
	t.Parallel()

	assert.NoError(t, New(slog.Default()).PeeringReady())

	r := start(t, Suffix("incus"), TTL(30))
	r.feed(t, append(hubFleetEvents(false), event(iutil.ActionSweepEnd, "", ""))...)

	answersWithin(t, r, "nobody.default.incus.", "10.146.11.6", dns.RcodeNameError, "warm at the round")

	r.stop(t)
}
