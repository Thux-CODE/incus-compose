package enricher

import (
	"sync"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// heldSessions is what Handle keeps when the inbox has no room, for Run to
// take as if it had come through the inbox: the connection controls, and the
// first event of a session newer than any seen at the door. Kept by session,
// not by arrival: what it holds is always of the newest session seen at the
// door, and an event of an older one never takes its place.
//
// The invariant it keeps is the one every consumer of the chain keeps (see
// sessionOf), at the door instead of the fold: the newest session seen at the
// door is never lost on the way in. An arrival of an older session is a
// leftover that changes nothing anywhere, and is not kept.
type heldSessions struct {
	mu sync.Mutex

	// newest is the newest session seen at the door, through the inbox or
	// not.
	newest uint64

	// events is what is kept, all of the session newest; at most two, the
	// last of them.
	events []*iutil.Event

	// ready says events holds something. One slot: a signal, not a count.
	ready chan struct{}
}

func newHeldSessions() *heldSessions {
	return &heldSessions{ready: make(chan struct{}, 1)}
}

// seen records the session of an event at the door, and reports whether it
// is newer than any seen before it.
func (h *heldSessions) seen(ev *iutil.Event) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ev.Session() <= h.newest {
		return false
	}

	h.newest = ev.Session()

	return true
}

// newestSeen is the newest session seen at the door: never older than any
// session a plugin after this one has seen, since everything reaches them
// through here.
func (h *heldSessions) newestSeen() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.newest
}

// keep holds ev, which the inbox had no room for, unless it is of a session
// older than the newest seen at the door. It reports whether ev was held.
func (h *heldSessions) keep(ev *iutil.Event) bool {
	h.mu.Lock()

	if ev.Session() < h.newest {
		h.mu.Unlock()

		return false
	}

	// Of the newest session, so whatever was held of an older one is a
	// leftover now.
	if len(h.events) > 0 && h.events[0].Session() < ev.Session() {
		h.events = h.events[:0]
	}

	h.events = append(h.events, ev)
	if len(h.events) > 2 {
		h.events = h.events[len(h.events)-2:]
	}

	h.mu.Unlock()

	select {
	case h.ready <- struct{}{}:
	default:
	}

	return true
}

// take is everything held, in the order it came, and holds nothing after.
func (h *heldSessions) take() []*iutil.Event {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := h.events
	h.events = nil

	return out
}
