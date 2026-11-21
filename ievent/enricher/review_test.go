package enricher

import (
	"log/slog"
	"maps"
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestReviewNetworkEventDuringRead: a network read taken before an update, a
// delete or a rename lands after it. The newer event wins: the update is read
// again, and the read of a network that went is abandoned rather than putting
// the old key back.
func TestReviewNetworkEventDuringRead(t *testing.T) {
	for _, action := range []string{incusapi.EventLifecycleNetworkUpdated, incusapi.EventLifecycleNetworkDeleted, incusapi.EventLifecycleNetworkRenamed} {
		t.Run(action, func(t *testing.T) {
			p := New(slog.Default())
			h := &fixture{reads: map[string]int{}, fleet: testlib.NewProject("p", 0, 1)}
			n := &h.fleet.Networks[0]
			n.Managed = true
			n.Config["ipv4.address"] = "10.1.0.1/24"
			oldName := n.Name
			p.reads.readNet = h.answerNet
			require.NoError(t, p.reads.start(2))
			defer p.reads.stop()
			ctx := t.Context()
			p.readNetwork(ctx, "p", oldName)
			old := <-p.reads.results
			require.NoError(t, old.err)
			// Keep an API reply as actually captured before the config update.
			snapshot := *old.network
			snapshot.Config = maps.Clone(snapshot.Config)
			old.network = &snapshot
			newName, previous := oldName, ""
			if action == incusapi.EventLifecycleNetworkRenamed {
				newName, previous = "newnet", oldName
				n.Name = newName
			}
			n.Config["ipv4.address"] = "10.2.0.1/24"
			p.acceptNetwork(ctx, iutil.NewEvent(time.Now(), action, "p", newName, previous))
			p.settleRead(ctx, old)
			select {
			case refresh := <-p.reads.results:
				p.settleRead(ctx, refresh)
			case <-time.After(60 * time.Millisecond):
			}
			net, _, exists := p.state.network("p", oldName)
			if action == incusapi.EventLifecycleNetworkUpdated {
				require.True(t, exists)
				require.Equal(t, "10.2.0.1/24", net.IPv4())
			} else {
				require.False(t, exists, "old network returned to the inventory")
			}
		})
	}
}
