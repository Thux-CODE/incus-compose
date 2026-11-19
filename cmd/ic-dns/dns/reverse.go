package dns

import (
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/miekg/dns"
)

// v6HostLabels is how many nibble labels of an ip6.arpa name belong to the host
// rather than its zone: 16 nibbles are the low 64 bits, the /64 Incus gives a bridge.
const v6HostLabels = 16

// reverseName returns the PTR owner name for addr, and the zone holding it. The
// zone comes from the address, so a subnet holding no instance is never claimed.
func reverseName(addr netip.Addr) (string, string, bool) {
	// dns.ReverseAddr reads a 4-in-6 address as IPv4 and answers under
	// in-addr.arpa, so the family has to be settled before both of them.
	addr = addr.Unmap()

	name, err := dns.ReverseAddr(addr.String())
	if err != nil {
		return "", "", false
	}

	labels := v6HostLabels
	if addr.Is4() {
		labels = 1
	}

	zone := name

	for range labels {
		cut := strings.IndexByte(zone, '.')
		if cut < 0 {
			return "", "", false
		}

		zone = zone[cut+1:]
	}

	return name, zone, true
}

// covered reports whether a network's own addressing reaches addr. A network
// with no prefixes covers nothing - what Incus reports for one it does not manage.
func covered(prefixes []netip.Prefix, addr netip.Addr) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}

	return false
}

// reverseClaims is the reverse zones a list of prefixes claims, each once, in
// the order the prefixes came in. See reverseZones for one prefix.
func reverseClaims(prefixes []netip.Prefix) []string {
	var out []string

	for _, prefix := range prefixes {
		for _, zone := range reverseZones(prefix) {
			if !slices.Contains(out, zone) {
				out = append(out, zone)
			}
		}
	}

	return out
}

// reverseZones is the reverse zones covering prefix, shaped like the ones
// reverseName builds, so a claim and a published zone are the same name.
//
// A prefix as narrow as a zone or narrower - /24 and up for IPv4, /64 and up
// for IPv6 - claims the one zone holding it, as an instance in it would. A
// wider one claims the zones at its own label boundary, an octet for IPv4 and a
// nibble for IPv6, rounded towards the narrower side so nothing outside the
// prefix is claimed: 10.147.0.0/16 is 147.10.in-addr.arpa., and 10.147.0.0/20
// the sixteen /24 zones inside it. That spreads a prefix over at most 128 zones.
func reverseZones(prefix netip.Prefix) []string {
	if !prefix.IsValid() {
		return nil
	}

	addr, bits := prefix.Addr(), prefix.Bits()

	// The same family reverseName settles on for an address in it.
	if addr.Is4In6() && bits >= 96 {
		addr, bits = addr.Unmap(), bits-96
	}

	step, widest := 4, 128-v6HostLabels*4
	if addr.Is4() {
		step, widest = 8, 24
	}

	bits = min(bits, widest)
	zoneBits := (bits + step - 1) / step * step

	base := netip.PrefixFrom(addr, bits).Masked().Addr().AsSlice()

	count := 1 << (zoneBits - bits)
	out := make([]string, 0, count)

	// The zones differ only in the bits between the prefix and the boundary,
	// which sit inside the last byte the boundary reaches into.
	last, shift := (zoneBits-1)/8, (8-zoneBits%8)%8

	for i := range count {
		raw := slices.Clone(base)
		if zoneBits > 0 {
			raw[last] |= byte(i << shift)
		}

		out = append(out, zoneName(raw, zoneBits, addr.Is4()))
	}

	return out
}

// zoneName is the reverse zone of the first zoneBits of raw: one label per octet
// under in-addr.arpa., one per nibble under ip6.arpa., most significant last.
func zoneName(raw []byte, zoneBits int, v4 bool) string {
	var labels []string

	if v4 {
		for i := zoneBits/8 - 1; i >= 0; i-- {
			labels = append(labels, strconv.Itoa(int(raw[i])))
		}

		return strings.Join(append(labels, "in-addr", "arpa"), ".") + "."
	}

	for n := zoneBits/4 - 1; n >= 0; n-- {
		nibble := raw[n/2] >> 4
		if n%2 == 1 {
			nibble = raw[n/2] & 0x0f
		}

		labels = append(labels, strconv.FormatUint(uint64(nibble), 16))
	}

	return strings.Join(append(labels, "ip6", "arpa"), ".") + "."
}
