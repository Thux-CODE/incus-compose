package dns

import (
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// TestSixthOlderSessionReadIsTheOnlyNews is the ic-dns half of
// TestSixthReadSettledAfterNewerSessionWentDownstream in the enricher: an
// event of session 3 the enricher dropped on a full inbox reaches the fold
// first, then the read the enricher settled while it still read for session
// 2, the only event that ever carries web (the enricher archived it, and
// trashes the same answer read again in session 3). Session 3's round ends.
// web is live in Incus and answers NXDOMAIN, warm, from here on.
func TestSixthOlderSessionReadIsTheOnlyNews(t *testing.T) {
	t.Skip("contradicts the session invariant (TestFifthOldDeleteAfterCurrentRound): ic-dns folds no data of an older session; the fix is in the enricher, which now never makes this sequence - see TestSixthReadSettledAfterNewerSessionWentDownstream")
	require.Equal(t, dns.RcodeSuccess, sixthWebAfterRound(t, 2), "a live instance answered NXDOMAIN after the round of session 3")
}

// TestSixthOlderSessionReadControl: the same read stamped 3 is folded. Passes;
// it is what makes the test above about the stamp alone.
func TestSixthOlderSessionReadControl(t *testing.T) {
	require.Equal(t, dns.RcodeSuccess, sixthWebAfterRound(t, 3))
}

func sixthWebAfterRound(t *testing.T, readSession uint64) int {
	t.Helper()

	c := newColdServer(t, Suffix("example"))

	c.p.fold(in(2, warm(event(iutil.ActionSweepEnd, "", ""))))
	c.p.fold(in(3, event(api.EventLifecycleInstanceUpdated, "shop", "other").WithDropped("enricher")))
	c.p.fold(in(readSession, eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", nil, map[string]string{"a": "10.0.0.2"})))
	c.p.fold(in(3, warm(event(iutil.ActionSweepEnd, "", ""))))
	c.p.write()

	w := &recorder{}
	c.a.ServeDNS(w, subnetQuery("web.shop.example.", "10.0.0.2"))

	return w.msg.Rcode
}
