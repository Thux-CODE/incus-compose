package dns

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// in stamps an event with the stream session the source minted it in.
func in(session uint64, ev *iutil.Event) *iutil.Event { return ev.WithSession(session) }

// TestSecondLostConnectionControls: the controls a full inbox drops go on
// past the fold, ahead of what is queued. The events carry the session the
// source minted them in, as it always stamps them, and that is what tells the
// stream is back - not which control arrived last.
//
// Adapted from the review: its events carried no session, which no event the
// source mints lacks; and "first listener never opened" is the source's to
// refuse (TestSecondFirstListenerNeverOpened), since a round with no stream
// open never reaches the chain.
func TestSecondLostConnectionControls(t *testing.T) {
	t.Run("connected dropped by DNS", func(t *testing.T) {
		c := newColdServer(t, InboxSize(1))
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.Handle(in(1, event("occupied", "", "")))
		c.p.Handle(in(2, event(iutil.ActionConnected, "", "")))
		c.p.fold(<-c.p.inbox)
		for range 3 {
			c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
		}
		require.Equal(t, 200, status())
		require.True(t, c.p.view.Ready(), "three successful rounds after reconnect")
	})
	t.Run("older disconnect delivered last to HTTP", func(t *testing.T) {
		c := newColdServer(t, InboxSize(1))
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, warm(event(iutil.ActionSweepEnd, "", ""))))
		require.Equal(t, 200, status())
		c.p.Handle(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.Handle(in(2, event(iutil.ActionConnected, "", "")))
		require.Equal(t, 503, status(), "the reconnection's round has not ended")
		c.p.fold(<-c.p.inbox)
		for range 3 {
			c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
		}
		require.Equal(t, 200, status())
		require.True(t, c.p.view.Ready())
	})
	t.Run("a round of the old session after the reconnection", func(t *testing.T) {
		c := newColdServer(t)
		status := readyAfter(t, c.p)
		c.p.fold(in(1, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, event(iutil.ActionDisconnected, "", "")))
		c.p.fold(in(2, event(iutil.ActionConnected, "", "")))
		c.p.fold(in(1, warm(event("instance-updated", "shop", "web"))))
		require.Equal(t, 503, status(), "a warm leftover of the session that went")
		require.False(t, c.p.view.Ready())
	})
}
