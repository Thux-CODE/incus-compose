package dns

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// in stamps an event with the stream session the source minted it in.
func in(session uint64, ev *iutil.Event) *iutil.Event { return ev.WithSession(session) }

// TestSecondSpillFIFO: one producer, an inbox of one, and a consumer taking
// the inbox and the spill as Run does. Every event is folded in arrival order.
func TestSecondSpillFIFO(t *testing.T) {
	c := newColdServer(t, InboxSize(1))
	const total = 150000
	events := make([]*iutil.Event, total)
	for i := range events {
		events[i] = iutil.NewEvent(time.Unix(int64(i+1), 0), "stored", "p", "web", "").WithRestored()
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i, ev := range events {
			c.p.Handle(ev)
			if i%4 == 3 {
				runtime.Gosched()
			}
		}
	}()
	var seen int
	var firstReordered string
	c.p.next = func(ev *iutil.Event) {
		if seen < total && ev != events[seen] && firstReordered == "" {
			firstReordered = fmt.Sprintf("arrival %d folded in position %d", ev.At().Unix(), seen+1)
		}
		seen++
	}
	for seen < total {
		select {
		case ev := <-c.p.inbox:
			c.p.fold(ev)
		case <-c.p.spilled:
			c.p.foldSpilled()
		}
	}
	<-finished
	require.Empty(t, firstReordered)
}

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
