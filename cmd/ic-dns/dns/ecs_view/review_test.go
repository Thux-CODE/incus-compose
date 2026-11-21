package ecs_view

import (
	"context"
	"log/slog"
	"testing"

	"github.com/coredns/coredns/plugin"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

// TestReviewTruncatedNegativeInsideClaim: before warm, a name under the
// suffix in no zone yet is asked of the forwarder, which answers NXDOMAIN with
// TC set. SERVFAIL, as for the same NXDOMAIN untruncated.
func TestReviewTruncatedNegativeInsideClaim(t *testing.T) {
	v := New(slog.Default())
	v.Claims = []string{"incus."}
	v.Next = plugin.HandlerFunc(func(_ context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
		m := new(dns.Msg)
		m.SetRcode(r, dns.RcodeNameError)
		m.Truncated = true

		return dns.RcodeSuccess, w.WriteMsg(m)
	})

	_, m := ask(t, v, "new.shop.incus.", dns.TypeA, "10.0.0.2")
	require.Equal(t, "SERVFAIL", dns.RcodeToString[m.Rcode])
}

// TestReviewSnapshotThenWarm: a query is suspended between its two loads
// while the source publishes the last name and turns warm. Whichever snapshot
// it reads, it must not take a warm it did not read with: the answer is the
// record, or SERVFAIL, never NXDOMAIN off a snapshot older than the warm.
//
// Not parallel: the hook is the package's.
func TestReviewSnapshotThenWarm(t *testing.T) {
	old := newBuilder().
		zone("shop.incus.", false).
		name("gateway.shop.incus.", ViewAnswer{apiView: a("gateway.shop.incus.", "10.0.1.10")}).
		at("10.0.1.0/24", apiView).
		done()

	whole := newBuilder().
		zone("shop.incus.", false).
		name("gateway.shop.incus.", ViewAnswer{apiView: a("gateway.shop.incus.", "10.0.1.10")}).
		name("last.shop.incus.", ViewAnswer{apiView: a("last.shop.incus.", "10.0.1.11")}).
		at("10.0.1.0/24", apiView).
		done()

	v := New(slog.Default())
	v.Claims = []string{"incus."}
	v.Replace(old)

	testHookAfterWarm = func() {
		testHookAfterWarm = nil

		v.Replace(whole)
		v.SetHealthy(true)
	}

	t.Cleanup(func() { testHookAfterWarm = nil })

	_, m := ask(t, v, "last.shop.incus.", dns.TypeA, "10.0.1.10")
	require.NotEqual(t, "NXDOMAIN", dns.RcodeToString[m.Rcode])
}
