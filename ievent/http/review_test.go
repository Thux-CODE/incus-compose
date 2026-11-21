package http

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestReviewDroppedDisconnectConcurrentWithFold: DNS hands on a disconnect it
// dropped from its caller's goroutine while its fold hands on a warm event
// from its own. Under -race this is the data race on lost; the decision is
// taken under one lock now.
func TestReviewDroppedDisconnectConcurrentWithFold(t *testing.T) {
	p := New(slog.Default())
	require.NoError(t, p.Setup(iutil.SetupArgs{Next: func(*iutil.Event) {}}))
	dropped := iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithDropped("dns")
	held := iutil.NewEvent(time.Now(), "instance-updated", "p", "web", "").WithChainState(iutil.ChainWarm)
	start := make(chan struct{})
	var wg sync.WaitGroup
	// DNS calls the same Next from Handle on overflow and from Run when folding.
	for _, ev := range []*iutil.Event{dropped, held} {
		wg.Go(func() {
			<-start
			for range 64 {
				p.Handle(ev)
			}
		})
	}
	close(start)
	wg.Wait()
}
