package dns

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// The session half of the third review's dns tests; the zone and peering half
// is another file's.

// TestThirdClosedSessionCannotBeReopenedByItsQueuedEvents: the connected and
// the end of round of session 1 are queued when its disconnected arrives. It
// spills behind them rather than going on ahead, and /ready, which sees them
// in order, ends closed.
func TestThirdClosedSessionCannotBeReopenedByItsQueuedEvents(t *testing.T) {
	c := newColdServer(t, InboxSize(2))
	status := readyAfter(t, c.p)
	c.p.Handle(in(1, event(iutil.ActionConnected, "", "")))
	c.p.Handle(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	c.p.Handle(in(1, event(iutil.ActionDisconnected, "", "")))
	require.Equal(t, 503, status())
	c.p.foldSpilled()
	require.Equal(t, 503, status(), "the same listener is still closed")
	require.False(t, c.p.view.Ready())
}

// TestThirdClosedSessionConnectedArrivingLate: the same, with the disconnected
// handed on ahead of the queue as a dropped control used to be. The connected
// of a session already seen closed reopens nothing.
func TestThirdClosedSessionConnectedArrivingLate(t *testing.T) {
	c := newColdServer(t)
	status := readyAfter(t, c.p)
	c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.Equal(t, 503, status())
	require.False(t, c.p.view.Ready())

	c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.Equal(t, 200, status(), "the next session's round ended")
	require.True(t, c.p.view.Ready())
}

// TestThirdConnectingAfterMissedDisconnectClearsFresh: the disconnected of
// session 1 never reached the fold; the connected of session 2 does. Session 2
// has not completed a round, so the view is not ready - until it has.
func TestThirdConnectingAfterMissedDisconnectClearsFresh(t *testing.T) {
	c := newColdServer(t, InboxSize(1))
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
	c.p.Handle(in(1, event("occupied", "", "")))
	c.p.Handle(in(1, event(iutil.ActionDisconnected, "", "")))
	c.p.foldSpilled()
	c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
	require.False(t, c.p.view.Ready(), "session two has not completed a sweep")

	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
}

// TestThirdMissedBothControlsClearsFresh: neither the disconnected of session 1
// nor the connected of session 2 got here; an event of session 2 does. The
// view is not ready until session 2's round.
func TestThirdMissedBothControlsClearsFresh(t *testing.T) {
	c := newColdServer(t)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
	c.p.fold(in(2, event("observed", "", "")))
	require.False(t, c.p.view.Ready(), "session two has not completed a sweep")

	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready())
}

// TestThirdExplicitSweepEndMustMatchCurrentSession: after session 2 came up,
// an end of round of session 1 that waited in a queue is no round.
func TestThirdExplicitSweepEndMustMatchCurrentSession(t *testing.T) {
	c := newColdServer(t)
	c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
	c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
	c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.False(t, c.p.view.Ready())

	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	require.True(t, c.p.view.Ready(), "session two's own round")
}
