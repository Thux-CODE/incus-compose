package debounce

import (
	"log/slog"
	"testing"
	"time"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestAnOlderControlNeverOvertakesANewerOne: the connected of session 3 is
// queued when the disconnected of session 2 finds the inbox full. Handed on
// at once it would reach the next plugin ahead of the connected of 3; it is a
// leftover, and goes nowhere. A control of the newest session, at a full
// inbox, still goes on at once, marked.
func TestAnOlderControlNeverOvertakesANewerOne(t *testing.T) {
	t.Parallel()

	var handed []*iutil.Event

	p := New(slog.Default(), InboxSize(1))

	err := p.Setup(iutil.SetupArgs{Wanted: wanted, Next: func(ev *iutil.Event) { handed = append(handed, ev) }})
	if err != nil {
		t.Fatalf("Setup: %s", err)
	}

	control := func(action string, session uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), action, "", "", "").WithSession(session)
	}

	p.Handle(control(iutil.ActionConnected, 3))
	p.Handle(control(iutil.ActionDisconnected, 2))

	if len(handed) != 0 {
		t.Fatalf("a control of session 2 went on ahead of the connected of 3: %v", handed[0].Action())
	}

	p.Handle(control(iutil.ActionDisconnected, 3))

	if len(handed) != 1 || handed[0].Session() != 3 || handed[0].Err() == nil {
		t.Fatalf("the disconnected of the newest session was not handed on, marked: %v", handed)
	}
}
