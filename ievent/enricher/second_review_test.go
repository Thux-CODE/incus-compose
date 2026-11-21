package enricher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestSecondRoundEndNamesItsSession: the run a connected restarts ends naming
// that connected's session, which is what the source checks it against.
func TestSecondRoundEndNamesItsSession(t *testing.T) {
	p := bare(t)
	p.reads.fleet = &fixture{}

	raised := make(chan iutil.Command, 1)
	p.args.CommandOut = raised

	p.accept(t.Context(), iutil.NewEvent(time.Now(), iutil.ActionConnected, "", "", "").WithSession(7))
	t.Cleanup(p.sweeperCancel)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone, run: p.sweepRun})

	cmd := <-raised
	require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
	require.Equal(t, uint64(7), cmd.Session)
}
