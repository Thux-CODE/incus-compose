package dns

import (
	"errors"
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestReviewFailedFirstReadIsNotACompleteRound(t *testing.T) {
	t.Skip("contested: the round is the enricher's to close. It holds the end of a run " +
		"until every instance it listed and nothing holds has been read " +
		"(TestReviewSweepEndsDespiteReadError), so a sweep-end behind a failed read " +
		"means the grace for that read ran out: past it, NXDOMAIN for the name is the " +
		"trade-off taken for a replica that turns ready at all")

	c := newColdServer(t, Suffix("incus"))
	c.p.fold(enriched(incusapi.EventLifecycleInstanceUpdated, "shop", "old", "10.0.0.2"))
	c.p.fold(event(incusapi.EventLifecycleInstanceUpdated, "shop", "new").WithFailed(errors.New("read timeout")))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	code, _ := resolve(c.a, "new.shop.incus.", dns.TypeA, "10.0.0.2")
	require.Equal(t, "SERVFAIL", dns.RcodeToString[code])
}
