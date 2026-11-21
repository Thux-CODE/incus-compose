// Package iutil is the vocabulary the source and its plugins share: the event, what can be read
// onto it, and the plugin interface. It knows the Incus API types, never Incus itself.
package iutil

import (
	"errors"
	"time"
)

// Event is what travels the chain, immutable by construction: no accessor hands back anything a
// caller can write through, so it is safe to share and hold past the walk that delivered it.
type Event struct {
	action string

	name    string
	oldName string

	projectName string

	err error

	// chainState is what the chain was when this event was made. See ChainState.
	chainState ChainState

	// restored says what is on the event came out of a cold store rather than a
	// read in this run. See Restored.
	restored bool

	// received is when the event was decoded.
	received time.Time

	// session is the stream session the source minted this event in. See
	// Session.
	session uint64

	// enriched says which reads landed.
	enriched Enrichment

	project  *Project
	network  *Network
	instance *Instance
}

// NewEvent builds one event, carrying no error and nothing read.
func NewEvent(received time.Time, action, project, name, oldName string) *Event {
	return &Event{
		received:    received,
		action:      action,
		projectName: project,
		name:        name,
		oldName:     oldName,
	}
}

// At is when the event was decoded off the stream, which is ahead of the walk.
func (e *Event) At() time.Time { return e.received }

// Action is the Incus lifecycle action, its own string rather than a vocabulary
// of ours, so classifying it stays the caller's business.
func (e *Event) Action() string { return e.action }

// ProjectName is the event's project, taken from the envelope rather than the
// payload, which project and profile events leave empty. What the project
// itself said is Project.
func (e *Event) ProjectName() string { return e.projectName }

// Name is what the action names.
func (e *Event) Name() string { return e.name }

// OldName is the pre-rename name, empty unless this is a rename. Only a caller
// knows which actions rename anything.
func (e *Event) OldName() string { return e.oldName }

// Err is why the event is finished with: ErrDropped, ErrFailed, or nil while it still acts.
func (e *Event) Err() error { return e.err }

// ChainState is what the whole chain was when this event was made, never what
// has happened to the event itself - that is Err.
func (e *Event) ChainState() ChainState { return e.chainState }

// WithChainState derives an event carrying the chain's state. The source stamps
// what it mints; a plugin minting one of its own carries over what it last saw.
func (e *Event) WithChainState(state ChainState) *Event {
	next := *e
	next.chainState = state

	return &next
}

// Session is the event stream session the source minted this event in,
// counted from 1 at each listener that opened; 0 is an event the source did
// not mint, or one minted before any listener opened. A connected names the
// session it opens and a disconnected the one it closes, so a plugin can tell
// which of two it was handed out of order is the newer, and that an event
// from a session after the last disconnected it saw means the stream came
// back even when the connected that said so was lost on the way.
func (e *Event) Session() uint64 { return e.session }

// WithSession derives an event minted in session. Only the source calls this.
func (e *Event) WithSession(session uint64) *Event {
	next := *e
	next.session = session

	return &next
}

// Restored says the instance on this event is what a previous run of the
// process held, put back from its cold store, and nothing in this run has read
// it yet. The first read that lands for it carries false, even where it found
// the same thing, so a consumer that must not act on a stale answer - a health
// status, say - can tell the two apart and wait for that one.
func (e *Event) Restored() bool { return e.restored }

// WithRestored derives an event marked as restored from the cold store. Only
// the plugin that owns the store calls this.
func (e *Event) WithRestored() *Event {
	next := *e
	next.restored = true

	return &next
}

// WithDropped marks the event finished with, naming who did it: a plugin's
// Name, or "source/<cause>". A no-op once the event is dropped or failed.
func (e *Event) WithDropped(by string) *Event {
	if e.err != nil {
		return e
	}

	next := *e
	next.err = ErrDropped.WithBy(by)

	return &next
}

// WithFailed marks the event as one the source could not complete. It outranks
// dropped; a second failure keeps the first.
func (e *Event) WithFailed(reason error) *Event {
	if e.err != nil {
		return e
	}

	next := *e
	next.err = ErrFailed.Wrap(reason)

	return &next
}

// Equal reports whether two events say the same thing about the same subject. At, ChainState
// and Restored are excluded: none of them is about the subject.
func (e *Event) Equal(other *Event) bool {
	if e == nil || other == nil {
		return e == other
	}

	return e.baseEqual(other) && e.instance.Equal(other.instance)
}

// EqualWithoutNets is Equal apart from where the instance sits, so one that has
// only moved compares equal. A consumer keying on addresses asks both, and takes
// the difference between the two answers as the move.
func (e *Event) EqualWithoutNets(other *Event) bool {
	if e == nil || other == nil {
		return e == other
	}

	return e.baseEqual(other) && e.instance.EqualNoNets(other.instance)
}

// baseEqual is what both comparisons share.
func (e *Event) baseEqual(other *Event) bool {
	return e.action == other.action &&
		e.name == other.name &&
		e.oldName == other.oldName &&
		e.projectName == other.projectName &&
		errors.Is(e.err, other.err) &&
		e.enriched == other.enriched &&
		e.project.Equal(other.project) &&
		e.network.equal(other.network)
}
