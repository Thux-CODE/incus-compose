package source

import (
	"context"
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestSecondOldRoundQueuedWhileOpeningStream: the end of the old run is queued
// while the source opens the listener, and applied after the connected it
// hands. It names no session of this stream, so it is dropped: the next event
// is still cold, since no read has even started for this connection.
func TestSecondOldRoundQueuedWhileOpeningStream(t *testing.T) {
	rec := newRecorder("enricher", nil)
	s := mustSource(t, rec)
	stream := make(chan api.Event)
	s.listen = func(context.Context) (<-chan api.Event, error) {
		s.raised <- iutil.Command{Action: iutil.ActionSweepEnd, ChainState: iutil.ChainWarm}
		return stream, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := runSource(ctx, s)
	t.Cleanup(func() { cancel(); <-done })
	require.Equal(t, iutil.ActionConnected, (<-rec.saw).Action())
	s.raised <- iutil.Command{Action: "observer"}
	for ev := range rec.saw {
		require.NotEqual(t, iutil.ActionSweepEnd, ev.Action(), "the old run's end walked")

		if ev.Action() == "observer" {
			require.Equal(t, iutil.ChainCold, ev.ChainState(), "no read has even started for this connection")
			return
		}
	}
}

// TestSecondRoundOfThisSessionTurnsWarm is the other half, the way out of
// cold: the end of the run the connected started names its session and turns
// the chain warm.
func TestSecondRoundOfThisSessionTurnsWarm(t *testing.T) {
	rec := newRecorder("enricher", nil)
	s := mustSource(t, rec)
	s.listen = (&listener{streams: []chan api.Event{make(chan api.Event)}}).open
	ctx, cancel := context.WithCancel(t.Context())
	done := runSource(ctx, s)
	t.Cleanup(func() { cancel(); <-done })

	connected := <-rec.saw
	require.Equal(t, iutil.ActionConnected, connected.Action())

	rec.args.CommandOut <- iutil.Command{Action: iutil.ActionSweepEnd, ChainState: iutil.ChainWarm, Session: connected.Session()}
	end := <-rec.saw
	require.Equal(t, iutil.ActionSweepEnd, end.Action())
	require.Equal(t, iutil.ChainWarm, end.ChainState())
	require.Equal(t, connected.Session(), end.Session())
}

// TestSecondFirstListenerNeverOpened: Incus refuses the stream while the run
// Run started reads the fleet over REST. Its end names no session, and none
// is open: dropped, so nothing downstream turns ready on a fleet whose events
// nobody is listening to.
func TestSecondFirstListenerNeverOpened(t *testing.T) {
	rec := newRecorder("enricher", nil)
	s := mustSource(t, rec)
	s.listen = (&listener{}).open
	ctx, cancel := context.WithCancel(t.Context())
	done := runSource(ctx, s)
	t.Cleanup(func() { cancel(); <-done })

	rec.args.CommandOut <- iutil.Command{Action: iutil.ActionSweepEnd, ChainState: iutil.ChainWarm}
	rec.args.CommandOut <- iutil.Command{Action: "observer"}

	ev := <-rec.saw
	require.Equal(t, "observer", ev.Action(), "a round ended with no stream ever open")
	require.Equal(t, iutil.ChainCold, ev.ChainState())
}
