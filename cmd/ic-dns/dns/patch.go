package dns

import (
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/miekg/dns"

	"github.com/lxc/incus-compose/cmd/ic-dns/dns/ecs_view"
)

// apply moves one instance from what the trees hold to what has arrived, taking
// back exactly what it put in. next nil is the instance gone.
func (s *state) apply(key string, next *instance, ttl uint32) {
	prev := s.held[key]

	// Dropped while its claim still stands, or the views it answered in are no
	// longer the views it is written under.
	if prev != nil {
		for name, answer := range s.contributions(prev, ttl) {
			s.dropName(name, answer)
		}

		s.moved(prev)

		for range prev.names {
			s.leave(prev.zone, prev.ns, ttl, true, prev.transfer)
		}

		for _, on := range prev.ptrs {
			s.leave(on.zone, prev.ns, ttl, true, prev.transfer)
		}

		for _, zone := range prev.aliasIn {
			s.leave(zone, nil, ttl, false, false)
		}

		s.dropAddrs(prev)

		freed, gone := s.release(prev)
		s.forget(gone, ttl)
		s.dropPrefixes(prev, freed)
	}

	if next == nil {
		delete(s.held, key)

		return
	}

	own, fresh := s.view(next)

	s.held[key] = next

	for name, answer := range s.contributions(next, ttl) {
		s.addName(name, answer)
	}

	for range next.names {
		s.enter(next.zone, next.ns, ttl, true, next.transfer)
	}

	for _, on := range next.ptrs {
		s.enter(on.zone, next.ns, ttl, true, next.transfer)
	}

	// After the zones its own names made, so an alias inside one of them finds
	// it served rather than inventing a second.
	next.aliasIn = make([]string, 0, len(next.alias))

	for _, name := range next.alias {
		zone := s.aliasZone(name)
		next.aliasIn = append(next.aliasIn, zone)
		s.enter(zone, nil, ttl, false, false)
	}

	s.moved(next)
	s.addAddrs(next, own)
	s.backfill(fresh, ttl)
}

// moved marks every zone this instance has a name in, so publish knows which
// to compare again.
func (s *state) moved(inst *instance) {
	for _, zone := range s.zonesOf(inst) {
		s.dirty(zone)
	}
}

// aliasZones is the zone each of this instance's aliases lands in.
func (s *state) aliasZones(inst *instance) []string {
	out := make([]string, 0, len(inst.alias))
	for _, name := range inst.alias {
		out = append(out, s.aliasZone(name))
	}

	return out
}

// step moves the serial of every zone whose content changed, and renders it
// under the new one. What a zone carries is compared, not what happened to it:
// a serial steps where the zone a transfer would carry (zoneDigest) differs
// from the one its serial was published with, and only there - a change that
// leaves the records as they were, a view moving or a read confirming the
// replay, steps nothing.
//
// Only once complete: before the first round a zone holds whatever the replay
// and the reads have reached, and a step is a claim the zone changed that
// nothing has confirmed. Digest is left as it was stored, so the first publish
// once complete compares every zone against what its serial was published
// with, and steps each that differs once - what the replay got wrong (a
// delete inside its window, an enricher store newer than this one) and what
// the round corrected alike.
//
// A zone nothing is left in stops being served, but keeps its serial for good,
// with a digest no content has: forgetting it would publish it at 1 when its
// names come back - a serial going backwards, and one a secondary that polled
// before may hold for the old content.
func (s *state) step(complete bool, ttl uint32) {
	first := complete && !s.compared

	// Before any digest is taken: which zone a name falls in is the longest
	// served, so a zone coming or going moves names out of or into the ones
	// above it, and those have to be compared on the zones that are served now.
	s.reparent()

	for name, held := range s.serials {
		if held.names <= 0 {
			if complete {
				held.Digest = emptyDigest
				held.Owed = false
				held.dirty = false
				s.serials[name] = held
			}

			continue
		}

		if !held.dirty && !first {
			continue
		}

		// Before complete a zone is rendered as it stands and left marked, so
		// the first compare takes it in whatever happened to it meanwhile.
		if !complete {
			// Its first publish is a birth rather than a step, and a
			// secondary takes zero as "no zone here". A zone stored by a
			// build that kept no digest takes none here: what its serial
			// described is the whole replay, which may land over several
			// windows and is only whole at the first compare. Whatever else
			// changes it meanwhile marks it owed (taint), and that is what a
			// cold store written now keeps.
			if held.Serial == 0 {
				held.Serial = 1
				held.born = true
			}

			s.serials[name] = held
			s.render(name, ttl)

			continue
		}

		held.dirty = false

		z, _ := s.zones.Get(ecs_view.NameKey(name))
		digest := zoneDigest(s.names.Root(), s.zones.Root(), z)

		switch {
		case held.Serial == 0:
			held.Serial = 1
			held.Digest = digest
		case held.born, held.Digest == "" && !held.Owed:
			// Born in this run before anything could transfer it, or stored by
			// a build that kept no digest and that nothing but the replay has
			// changed since: what it holds now is the replay, confirmed or
			// not, and that is what the serial is taken to describe. One owed
			// has no digest to equal and steps below.
			held.Digest = digest
			held.born = false
		case digest != held.Digest:
			held.Serial++
			held.Digest = digest
			held.Owed = false
		}

		s.serials[name] = held
		s.render(name, ttl)
	}

	if complete {
		s.compared = true
		s.replay = nil
	}
}

// reparent takes out of the zone tree every zone nothing is left in, and marks
// as moved every zone above one that came to be served or stopped being: a
// transfer carries a name in the longest zone served over it (zoneRecords), so
// a child zone appearing takes its names - an alias counted in the parent
// included - out of what the parent carries, and one going gives them back.
// Without this the parent's content changes under the serial it holds, and a
// secondary at that serial keeps the old records.
func (s *state) reparent() {
	for name, held := range s.serials {
		serving := held.names > 0
		if !serving {
			s.zones.Delete(ecs_view.NameKey(name))
		}

		if serving == held.served {
			continue
		}

		held.served = serving
		s.serials[name] = held

		// What changed the zones above is whatever made this one come or go.
		above := name
		for i := strings.IndexByte(above, '.'); i >= 0 && i+1 < len(above); i = strings.IndexByte(above, '.') {
			above = above[i+1:]
			s.dirty(above)

			if held.changed {
				s.taint(above)
			}
		}
	}
}

// aliasZone is the zone one alias lands in: the longest served, otherwise its
// own parent. Decided on arrival, so a longer zone appearing later leaves this
// count stale - ZoneOf takes the longest, so the answers are not, and a
// transfer carries the alias in the longest too (zoneRecords). The count is
// taken back out of the zone it went into (instance.aliasIn), never out of
// one chosen again later: a child zone born since never got it, and losing it
// there takes a zone that still has hosts off the air.
func (s *state) aliasZone(name string) string {
	zone, _ := aliasZone(name, func(zone string) bool {
		held, known := s.serials[zone]

		// A zone kept for its serial alone holds nothing to be served.
		return known && held.names > 0
	})

	return zone
}

// contributions is every name this instance answers to and what it puts under
// each: its own and its service name, the aliases it claims, and the reverse
// names its addresses answer to.
func (s *state) contributions(inst *instance, ttl uint32) map[string]ecs_view.ViewAnswer {
	out := make(map[string]ecs_view.ViewAnswer, len(inst.names)+len(inst.alias)+len(inst.ptrs))

	for _, name := range inst.names {
		// Written for every replica at once, by refresh.
		if name == inst.service || name == inst.standby {
			continue
		}

		out[name] = s.records(inst, name, ttl)
	}

	for _, name := range inst.alias {
		out[name] = s.alias(inst, name, ttl)
	}

	for name, on := range inst.ptrs {
		out[name] = s.reverse(inst, name, on.key, ttl)
	}

	return out
}

// refresh writes every service name from the replicas that carry it, as pick
// chooses them. A drained replica carries no service name, so it is never among
// them.
//
// Rebuilt from everything held rather than patched per instance, since one
// replica becoming ready changes what the others contribute. The one way in, so
// whatever else rewrites views calls this too. It reports whether any name was
// rewritten.
//
// A name is rewritten where its answer changed view by view, but its zone is
// marked as moved only where what a transfer carries changed: the records
// flattened over every view. A change in which view sees what is not a change
// to the zone, and stepping its serial for one would have every secondary
// transfer it back.
func (s *state) refresh(ttl uint32) bool {
	carriers := map[string][]*instance{}
	zones := map[string]string{}

	// A drained replica named after its service answers to that name as its
	// own only where no other replica carries it.
	standby := map[string][]*instance{}

	for _, inst := range s.held {
		if inst.standby != "" {
			standby[inst.standby] = append(standby[inst.standby], inst)
			zones[inst.standby] = inst.zone
		}

		if inst.service == "" {
			continue
		}

		carriers[inst.service] = append(carriers[inst.service], inst)
		zones[inst.service] = inst.zone
	}

	for name, replicas := range standby {
		if _, carried := carriers[name]; !carried {
			carriers[name] = replicas
		}
	}

	changed := false

	for name, held := range s.served {
		_, still := carriers[name]
		if still {
			continue
		}

		s.dropName(name, held)
		s.ownBack(name, ttl)
		delete(s.served, name)
		s.dirty(s.servedZone[name])
		delete(s.servedZone, name)

		changed = true
	}

	for name, replicas := range carriers {
		next := s.service(name, replicas, ttl)

		prev := s.served[name]
		if sameAnswer(prev, next) {
			continue
		}

		if prev != nil {
			s.dropName(name, prev)
			s.ownBack(name, ttl)
		}

		s.addName(name, next)
		s.served[name] = next
		s.servedZone[name] = zones[name]

		if !sameRecords(flatten(prev), flatten(next)) {
			s.dirty(zones[name])
		}

		changed = true
	}

	return changed
}

// ownBack writes one name again for every instance answering to it as its own.
// A service answer is taken out record by record, and the records an
// instance answers its own name with are the same ones: an instance whose
// service was named after it, renamed, has just written its own name back as
// the old service answer is dropped, and would lose it with it.
func (s *state) ownBack(name string, ttl uint32) {
	for _, inst := range s.held {
		if name == inst.service || name == inst.standby || !slices.Contains(inst.names, name) {
			continue
		}

		s.addName(name, s.records(inst, name, ttl))
	}
}

// service is what one service name answers, view by view: in each view, the
// replicas pick chooses among those with an address there. A replica with none
// in a view - stopped, paused, not leased yet, or on other networks - is no
// candidate in it, so it can neither win readiness there and leave the name
// empty, nor keep a replica the view does reach out of it.
func (s *state) service(name string, replicas []*instance, ttl uint32) ecs_view.ViewAnswer {
	type offer struct {
		inst    *instance
		records ecs_view.Records
	}

	offers := map[ecs_view.ViewID][]offer{}

	out := ecs_view.ViewAnswer{}

	for _, inst := range replicas {
		for id, records := range s.records(inst, name, ttl) {
			if len(records) == 0 {
				// The name is there and holds nothing, as a replica with no
				// lease yet always left it.
				if _, held := out[id]; !held {
					out[id] = ecs_view.Records{}
				}

				continue
			}

			offers[id] = append(offers[id], offer{inst: inst, records: records})
		}
	}

	// A querier is answered from its view and its peer half at once, so the
	// two are one choice: a replica starting on the querier's own network
	// does not answer beside a healthy one it reaches through a peer.
	done := map[ecs_view.ViewID]bool{}

	for id := range offers {
		own := ecs_view.OwnOf(id)
		if done[own] {
			continue
		}

		done[own] = true

		halves := []ecs_view.ViewID{own, ecs_view.PeerOf(own)}

		var candidates []*instance

		for _, half := range halves {
			for _, o := range offers[half] {
				if !slices.Contains(candidates, o.inst) {
					candidates = append(candidates, o.inst)
				}
			}
		}

		answering := pick(candidates)

		for _, half := range halves {
			for _, o := range offers[half] {
				if slices.Contains(answering, o.inst) {
					out[half] = union(out[half], o.records)
				}
			}
		}
	}

	return out
}

// pick is who answers for one service, out of its candidates:
//
//  1. the ready ones this run has read;
//  2. where there are none, the ready ones the cold store restored and no read
//     has confirmed yet - what a previous run last saw, address and health
//     included;
//  3. where there are none of those either, every one of them.
//
// A read in this run outranks a restored one because a restored address may
// have moved while this process was down. The last step is fail-open:
// readiness picks between replicas, it never takes a service off the air.
func pick(replicas []*instance) []*instance {
	var fresh, restored []*instance

	for _, inst := range replicas {
		switch {
		case !inst.ready:
		case inst.Restored():
			restored = append(restored, inst)
		default:
			fresh = append(fresh, inst)
		}
	}

	switch {
	case len(fresh) > 0:
		return fresh
	case len(restored) > 0:
		return restored
	default:
		return replicas
	}
}

// dirty marks one zone as moved, so publish compares it again.
func (s *state) dirty(zone string) {
	held, known := s.serials[zone]
	if !known {
		return
	}

	held.dirty = true
	s.serials[zone] = held

	if s.reading {
		s.taint(zone)
	}
}

// taint marks a zone as changed by something other than the replay, before
// the first compare: a zone stored by a build that kept no digest is owed a
// step then, since what its serial described - the replay - is no longer
// what it holds. After the first compare a digest says it all, and this is
// nothing.
func (s *state) taint(zone string) {
	if s.compared {
		return
	}

	held, known := s.serials[zone]
	if !known {
		return
	}

	held.changed = true

	if held.Serial != 0 && !held.born && held.Digest == "" {
		held.Owed = true
	}

	s.serials[zone] = held
}

// zonesOf is every zone one instance has a name in: where each alias is
// carried now, and where it was counted, which a zone born since may have
// parted.
func (s *state) zonesOf(inst *instance) []string {
	out := append([]string{inst.zone}, s.aliasZones(inst)...)
	out = append(out, inst.aliasIn...)

	for _, on := range inst.ptrs {
		out = append(out, on.zone)
	}

	return out
}

// sameAnswer reports whether two answers for one name say the same thing in
// every view. union sorts what it builds, so equal answers line up record for
// record.
func sameAnswer(a, b ecs_view.ViewAnswer) bool {
	if len(a) != len(b) {
		return false
	}

	for id, records := range a {
		other, ok := b[id]
		if !ok || !sameRecords(records, other) {
			return false
		}
	}

	return true
}

// sameRecords reports whether two sets of records hold the same ones, type by
// type, each sorted the way union leaves it.
func sameRecords(a, b ecs_view.Records) bool {
	if len(a) != len(b) {
		return false
	}

	for qtype, rrs := range a {
		if !slices.EqualFunc(rrs, b[qtype], sameRR) {
			return false
		}
	}

	return true
}

// flatten is every record of an answer whatever view it is in, which is what a
// zone transfer carries.
func flatten(answer ecs_view.ViewAnswer) ecs_view.Records {
	out := ecs_view.Records{}

	for _, records := range answer {
		out = union(out, records)
	}

	return out
}

// backfill writes every name already on these wires under a view that did not
// exist until now. The rare path, and the only one that is not one instance's:
// a view is a set of networks, so everything reachable in it answers there.
func (s *state) backfill(fresh []ecs_view.ViewID, ttl uint32) {
	for _, id := range fresh {
		for _, inst := range s.on(ecs_view.KeysOf(id)) {
			for name, answer := range s.contributions(inst, ttl) {
				records, answers := answer[id]
				if !answers {
					continue
				}

				s.addName(name, ecs_view.ViewAnswer{id: records})
			}
		}

		// And its peer half, from everything on the networks it is peered with.
		peer := ecs_view.PeerOf(id)

		for _, inst := range s.on(s.peerKeys(id)) {
			for name, answer := range s.contributions(inst, ttl) {
				records, answers := answer[peer]
				if !answers {
					continue
				}

				s.addName(name, ecs_view.ViewAnswer{peer: records})
			}
		}
	}
}

// forget takes a view nothing is left in off every name that answered there.
func (s *state) forget(gone []ecs_view.ViewID, ttl uint32) {
	for _, id := range gone {
		for _, inst := range s.on(ecs_view.KeysOf(id)) {
			for name := range s.contributions(inst, ttl) {
				s.dropView(name, id)
			}
		}

		peer := ecs_view.PeerOf(id)

		for _, inst := range s.on(s.peerKeys(id)) {
			for name := range s.contributions(inst, ttl) {
				s.dropView(name, peer)
			}
		}
	}
}

// on is every instance held on any of these networks.
func (s *state) on(keys []string) []*instance {
	var out []*instance

	for _, inst := range s.held {
		for _, key := range keys {
			_, sits := inst.nets[key]
			if sits {
				out = append(out, inst)

				break
			}
		}
	}

	return out
}

// dropView takes one view off a name entirely, and the name with it where that
// was the only view answering.
func (s *state) dropView(name string, id ecs_view.ViewID) {
	key := ecs_view.NameKey(name)

	held, ok := s.names.Get(key)
	if !ok {
		return
	}

	_, answers := held[id]
	if !answers {
		return
	}

	out := ecs_view.ViewAnswer{}

	for other, records := range held {
		if other == id {
			continue
		}

		out[other] = records
	}

	if len(out) == 0 {
		s.names.Delete(key)

		return
	}

	s.names.Insert(key, out)
}

// enter counts one name into its zone, rendering the zone where it is the first.
func (s *state) enter(name string, ns []string, ttl uint32, host, transfer bool) {
	held := s.serials[name]

	held.names++

	if host {
		held.hosts++
	}

	if transfer {
		held.transfers++
	}

	if held.ns == nil {
		held.ns = map[string]int{}
	}

	for _, server := range ns {
		held.ns[server]++
	}

	s.serials[name] = held
	s.render(name, ttl)
}

// leave counts one name back out, taking the zone with it where that was the
// last name in it.
func (s *state) leave(name string, ns []string, ttl uint32, host, transfer bool) {
	held, known := s.serials[name]
	if !known {
		return
	}

	held.names--

	if host {
		held.hosts--
	}

	if transfer {
		held.transfers--
	}

	for _, server := range ns {
		held.ns[server]--
		if held.ns[server] <= 0 {
			delete(held.ns, server)
		}
	}

	// Not removed here: an update leaves its zone and enters it again, and
	// deleting between the two would lose the serial and republish it at 1.
	// Publish is what sweeps a zone nothing came back to.
	held.dirty = true

	s.serials[name] = held

	if held.names > 0 {
		s.render(name, ttl)
	}
}

// render writes one zone's SOA and NS, which answering a denial reads and never
// synthesizes.
func (s *state) render(name string, ttl uint32) {
	held := s.serials[name]

	servers := make([]string, 0, len(held.ns))
	for server := range held.ns {
		servers = append(servers, server)
	}

	sort.Strings(servers)

	s.zones.Insert(ecs_view.NameKey(name), zone(name, held, servers, ttl))
}

// reverse is one instance's PTR for one reverse name. Scoped to the network the
// address sits on, so a PTR obeys the same reachability its forward name does.
func (s *state) reverse(inst *instance, name, on string, ttl uint32) ecs_view.ViewAnswer {
	out := ecs_view.ViewAnswer{}

	for _, id := range s.views(on) {
		out[id] = renderPTR(name, []string{inst.names[0]}, ttl)
	}

	// A view peered with that network reads the PTR the way it reads the name.
	for _, id := range s.peerViews(on) {
		out[ecs_view.PeerOf(id)] = renderPTR(name, []string{inst.names[0]}, ttl)
	}

	return out
}

// alias is one instance's CNAME for a name it claims, chased into the records
// its own name answers with so answering through one is the same lookup.
func (s *state) alias(inst *instance, name string, ttl uint32) ecs_view.ViewAnswer {
	cname := dns.RR(&dns.CNAME{
		Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: ttl},
		Target: inst.names[0],
	})

	out := ecs_view.ViewAnswer{}

	for id, records := range s.records(inst, inst.names[0], ttl) {
		chased := make(ecs_view.Records, len(records)+1)
		chased[dns.TypeCNAME] = []dns.RR{cname}

		for qtype, rrs := range records {
			chased[qtype] = append([]dns.RR{cname}, rrs...)
		}

		out[id] = chased
	}

	return out
}

// addAddrs files every address this instance claims under the view it queries
// from, and every prefix of its networks under that network's own view.
func (s *state) addAddrs(inst *instance, own ecs_view.ViewID) {
	for key, net := range inst.nets {
		for _, addr := range net.ipv4 {
			s.index(netip.PrefixFrom(addr, addr.BitLen()), own)
		}

		for _, addr := range net.ipv6 {
			s.index(netip.PrefixFrom(addr, addr.BitLen()), own)
		}

		anon := ecs_view.ViewOf([]string{key})
		for _, prefix := range net.prefixes {
			s.index(prefix, anon)
		}
	}
}

// dropAddrs takes this instance's own addresses back out. The network prefixes
// are not its own to remove; dropPrefixes does those when the wire empties.
func (s *state) dropAddrs(inst *instance) {
	for _, net := range inst.nets {
		for _, addr := range net.ipv4 {
			s.unindex(netip.PrefixFrom(addr, addr.BitLen()))
		}

		for _, addr := range net.ipv6 {
			s.unindex(netip.PrefixFrom(addr, addr.BitLen()))
		}
	}
}

// dropPrefixes takes a wire's own prefixes out once the last instance on it has
// gone.
func (s *state) dropPrefixes(inst *instance, freed []string) {
	for _, key := range freed {
		net, sits := inst.nets[key]
		if !sits {
			continue
		}

		for _, prefix := range net.prefixes {
			s.unindex(prefix)
		}
	}
}

// index files one prefix under the view a querier inside it belongs to. Two
// claiming one refuse rather than let the last writer decide.
func (s *state) index(prefix netip.Prefix, view ecs_view.ViewID) {
	key, v4 := ecs_view.AddrKey(prefix)

	tree := &s.v6
	if v4 {
		tree = &s.v4
	}

	held, taken := tree.Get(key)
	if taken && held != view {
		tree.Set(key, ecs_view.AmbiguousView)

		return
	}

	tree.Set(key, view)
}

// unindex takes one prefix out. An ambiguous one stays ambiguous: the value is
// one view, so what is left after a claimant goes is not in it.
func (s *state) unindex(prefix netip.Prefix) {
	key, v4 := ecs_view.AddrKey(prefix)

	tree := &s.v6
	if v4 {
		tree = &s.v4
	}

	tree.Delete(key)
}

// records is one instance's records for one name, by the view each is reachable
// in. A view spanning several networks answers with what it can see of all of
// them, which is why this is not per network.
func (s *state) records(inst *instance, name string, ttl uint32) ecs_view.ViewAnswer {
	out := ecs_view.ViewAnswer{}

	for key := range inst.nets {
		for _, id := range s.views(key) {
			_, done := out[id]
			if done {
				continue
			}

			var v4, v6 []netip.Addr

			for _, on := range ecs_view.KeysOf(id) {
				net, reachable := inst.nets[on]
				if !reachable {
					continue
				}

				v4 = append(v4, net.ipv4...)
				v6 = append(v6, net.ipv6...)
			}

			// Sorted, so one fleet renders one way however its events arrived.
			slices.SortFunc(v4, netip.Addr.Compare)
			slices.SortFunc(v6, netip.Addr.Compare)

			out[id] = render(name, v4, v6, ttl)
		}

		// The views that see this network through a peer, under their peer half,
		// with what they reach of this instance that way.
		for _, id := range s.peerViews(key) {
			peer := ecs_view.PeerOf(id)

			_, done := out[peer]
			if done {
				continue
			}

			var v4, v6 []netip.Addr

			for _, on := range s.peerKeys(id) {
				net, reachable := inst.nets[on]
				if !reachable {
					continue
				}

				v4 = append(v4, net.ipv4...)
				v6 = append(v6, net.ipv6...)
			}

			slices.SortFunc(v4, netip.Addr.Compare)
			slices.SortFunc(v6, netip.Addr.Compare)

			out[peer] = render(name, v4, v6, ttl)
		}
	}

	return out
}

// addName folds one instance's records for a name into what the tree holds.
func (s *state) addName(name string, add ecs_view.ViewAnswer) {
	key := ecs_view.NameKey(name)

	out := ecs_view.ViewAnswer{}

	held, ok := s.names.Get(key)
	if ok {
		for id, records := range held {
			out[id] = records
		}
	}

	for id, records := range add {
		out[id] = union(out[id], records)
	}

	// An instance that lost its addresses contributes nothing, and a name with
	// nothing under it is NODATA where it should be gone.
	if len(out) == 0 {
		s.names.Delete(key)

		return
	}

	s.names.Insert(key, out)
}

// dropName takes one instance's records back out, and the name with them where
// nothing else was answering there.
func (s *state) dropName(name string, drop ecs_view.ViewAnswer) {
	key := ecs_view.NameKey(name)

	held, ok := s.names.Get(key)
	if !ok {
		return
	}

	out := ecs_view.ViewAnswer{}

	for id, records := range held {
		gone, touched := drop[id]
		if !touched {
			out[id] = records

			continue
		}

		left := without(records, gone)
		if len(left) > 0 {
			out[id] = left
		}
	}

	if len(out) == 0 {
		s.names.Delete(key)

		return
	}

	s.names.Insert(key, out)
}

// union is one view's records with another's folded in, built fresh so a map
// already published is never written through.
func union(held, add ecs_view.Records) ecs_view.Records {
	out := make(ecs_view.Records, len(held)+len(add))

	for qtype, rrs := range held {
		out[qtype] = slices.Clone(rrs)
	}

	for qtype, rrs := range add {
		for _, rr := range rrs {
			if slices.ContainsFunc(out[qtype], func(have dns.RR) bool { return sameRR(have, rr) }) {
				continue
			}

			out[qtype] = append(out[qtype], rr)
		}

		sortRRs(out[qtype])
	}

	return out
}

// without is one view's records with another's taken out, dropping a type
// nothing is left under so a name with none at all can be told.
func without(held, gone ecs_view.Records) ecs_view.Records {
	out := make(ecs_view.Records, len(held))

	for qtype, rrs := range held {
		var left []dns.RR

		for _, rr := range rrs {
			if slices.ContainsFunc(gone[qtype], func(have dns.RR) bool { return sameRR(have, rr) }) {
				continue
			}

			left = append(left, rr)
		}

		if len(left) > 0 {
			out[qtype] = left
		}
	}

	return out
}

// sameRR reports whether two records say the same thing. By value, because a
// patch renders its own: contains compares by identity and would miss.
func sameRR(a, b dns.RR) bool {
	if a.Header().Rrtype != b.Header().Rrtype || a.Header().Name != b.Header().Name {
		return false
	}

	switch x := a.(type) {
	case *dns.A:
		y, ok := b.(*dns.A)

		return ok && x.A.Equal(y.A)
	case *dns.AAAA:
		y, ok := b.(*dns.AAAA)

		return ok && x.AAAA.Equal(y.AAAA)
	case *dns.PTR:
		y, ok := b.(*dns.PTR)

		return ok && x.Ptr == y.Ptr
	case *dns.CNAME:
		y, ok := b.(*dns.CNAME)

		return ok && x.Target == y.Target
	}

	return false
}

// sortRRs orders one type's records, so a name reads the same however the
// events that built it arrived.
func sortRRs(rrs []dns.RR) {
	sort.Slice(rrs, func(i, j int) bool { return rrs[i].String() < rrs[j].String() })
}
