package dns

import (
	"net/netip"
	"testing"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
)

func TestFourthChildKeepsItsLastHostAfterParentAliasGoes(t *testing.T) {
	const child = "api.shop.example."
	c := newColdServer(t, Suffix("example"), AllowTransfer([]netip.Prefix{netip.MustParsePrefix("192.0.2.53/32")}))
	parent := eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"})
	c.p.fold(parent)
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	worker := eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: child}, map[string]string{"a": "10.0.0.3"})
	c.p.fold(worker.WithProject(iutil.NewProject(optedIn())))
	c.p.write()
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, xfrPeer(true), xfrQuery(child, 0)))
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "web"))
	c.p.write()
	require.Equal(t, dns.RcodeSuccess, serve(t, c.p.xfr, xfrPeer(true), xfrQuery(child, 0)), "zone %s still has worker", child)
}

// The parent keeps the count of the alias it got while the child carries it,
// and the child going gives it back: the alias then leaves the parent, which
// it never left, and nothing is counted twice or below zero.
func TestFourthParentAliasCountedOnceAcrossChild(t *testing.T) {
	const zone = "shop.example."
	c := newColdServer(t, Suffix("example"))
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "shop", "web", map[string]string{metaAliases: "v1.api"}, map[string]string{"a": "10.0.0.2"}))
	c.p.fold(warm(event(iutil.ActionSweepEnd, "", "")))
	names := c.p.state.serials[zone].names
	c.p.fold(eventOn(api.EventLifecycleInstanceUpdated, "other", "worker", map[string]string{metaZone: "api." + zone}, map[string]string{"a": "10.0.0.3"}))
	c.p.write()
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "other", "worker"))
	c.p.write()
	require.Equal(t, names, c.p.state.serials[zone].names)
	require.Zero(t, c.p.state.serials["api."+zone].names)
	c.p.fold(event(api.EventLifecycleInstanceDeleted, "shop", "web"))
	c.p.write()
	require.Zero(t, c.p.state.serials[zone].names)
}
