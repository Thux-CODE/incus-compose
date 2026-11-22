package dns

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// The session half of the fourth review's dns tests; the zone half is another
// file's.

// TestFourthClosedSessionWatermark: the disconnected of session 3, then the
// disconnected of session 2 late out of a queue, then the connected and an end
// of round of session 3. Session 3 closed, and stays closed: nothing of it, or
// older, reopens it.
func TestFourthClosedSessionWatermark(t *testing.T) {
	c := newColdServer(t)
	status := readyAfter(t, c.p)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(3, event(iutil.ActionDisconnected, "", "")))
	c.p.fold(in(2, event(iutil.ActionDisconnected, "", "")))
	c.p.fold(in(3, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(3, warm(event(iutil.ActionSweepEnd, "", ""))))
	t.Logf("view ready=%v, HTTP=%d", c.p.view.Ready(), status())
	require.Equal(t, 503, status())
	require.False(t, c.p.view.Ready())

	// The way out: the next session's own round.
	c.p.fold(in(4, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(4, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.Equal(t, 200, status())
	require.True(t, c.p.view.Ready())
}

// TestFourthPeerEventOfNewSession: a peer event is the first news of session
// 2. It opens the session not fresh, as any other event would.
func TestFourthPeerEventOfNewSession(t *testing.T) {
	c := newColdServer(t, PeerViews(true))
	c.p.peersRead.Store(true)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
	c.p.fold(in(2, event(peerActions[0], "p", "net")))
	require.False(t, c.p.view.Ready())

	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready(), "session two's own round")
}

// TestFourthDelayedConnectedAfterOwnRound: session 2 is opened by an ordinary
// event, its round ends, then its connected arrives late. Session 2 is open
// already: the connected changes nothing, in /ready either.
func TestFourthDelayedConnectedAfterOwnRound(t *testing.T) {
	c := newColdServer(t)
	status := readyAfter(t, c.p)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
	connected := in(2, event(iutil.ActionConnected, "", ""))
	c.p.fold(in(2, event("observed", "", "")))
	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.Equal(t, 200, status())
	raised := make(chan iutil.Command, 8)
	c.p.commandOut = raised
	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))).WithDropped("enricher"))
	c.p.fold(connected)
	t.Logf("after late connected: HTTP=%d, view ready=%v, recovery commands=%d", status(), c.p.view.Ready(), len(raised))
	require.True(t, c.p.view.Ready())
	require.Equal(t, 200, status(), "the round already ended on this listener")
}

// TestFourthPriorDisconnectAfterNewListener: session 2 is up and has had its
// round when the disconnected of session 1 arrives. It is a leftover.
func TestFourthPriorDisconnectAfterNewListener(t *testing.T) {
	c := newColdServer(t)
	status := readyAfter(t, c.p)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
	c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
	require.False(t, c.p.view.Ready())
	require.Equal(t, 503, status())
	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
	require.True(t, c.p.view.Ready())
	require.Equal(t, 200, status())
}
