package enricher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestFourthRunSessionAfterDrainingInbox: the connected of session 2 waits in
// the controls slot, behind it the disconnected of session 2; the connected of
// session 3 goes through the inbox. Whichever Run takes first, session 3 is
// the newest, session 2 a leftover: every round from then on names 3.
func TestFourthRunSessionAfterDrainingInbox(t *testing.T) {
	p := New(slog.Default(), InboxSize(1), SweepInterval(20*time.Millisecond), ReadDelay(0))
	f := &fixture{fleet: testlib.NewProject("p", 0, 0)}
	p.reads.fleet = f
	p.reads.readProject = f.GetProject
	commands := make(chan iutil.Command, 16)
	p.args.CommandOut = commands
	p.args.Next = func(*iutil.Event) {}
	control := func(action string, n uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), action, "", "", "").WithChainState(iutil.ChainCold).WithSession(n)
	}

	// The consumer has taken the first control, freeing one inbox slot.
	p.Handle(control(iutil.ActionDisconnected, 1))
	p.Handle(control(iutil.ActionConnected, 2))
	p.Handle(control(iutil.ActionDisconnected, 2))
	initial := <-p.inbox
	p.sweepSession = 1
	p.accept(t.Context(), initial)
	p.Handle(control(iutil.ActionConnected, 3))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	var sessions []uint64
	deadline := time.After(2 * time.Second)
	for len(sessions) < 4 {
		select {
		case cmd := <-commands:
			require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
			sessions = append(sessions, cmd.Session)
		case <-deadline:
			t.Fatalf("missing round completions: %v", sessions)
		}
	}
	require.Equal(t, []uint64{3, 3}, sessions[2:], "actual sweeper completions: %v", sessions)
}

// TestFourthDisconnectAtCapacity: a disconnected the inbox has no room for is
// not handed on dropped, ahead of what is queued: it waits with the controls.
func TestFourthDisconnectAtCapacity(t *testing.T) {
	p := bare(t)
	p.inbox = make(chan *iutil.Event, 1)
	p.Handle(iutil.NewEvent(time.Now(), "occupied", "", "", ""))
	p.args.Next = func(ev *iutil.Event) {
		require.NoError(t, ev.Err(), "the downstream plugin received a dropped connection control")
	}
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithSession(1))
	require.Equal(t, iutil.ActionDisconnected, (<-p.controls).Action())
}
