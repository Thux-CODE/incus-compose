package dns

import (
	"maps"
	"slices"
	"sort"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"github.com/koji-hirono/go-critbit"

	"github.com/lxc/incus-compose/cmd/ic-dns/dns/ecs_view"
)

// state is what folding owns. Run's goroutine alone, so nothing here locks.
type state struct {
	// held is every instance the trees were written from, keyed by project and
	// name. What is in them, not what has arrived.
	held map[string]*instance

	// pending is what has arrived since the last write, nil where the instance
	// went. Written once per window, so an instance that moved and moved back
	// inside one reaches the trees not at all.
	pending map[string]*instance

	// nets is which views are over a wire, and what keeps each alive - the one
	// thing the trees cannot answer, since a wire is not a name.
	nets map[string]map[ecs_view.ViewID]int

	// peers is which networks each network is peered with, both ways round and
	// never transitively: what a view sees past its own networks. Empty sees
	// nothing past them, which is the fold without peer views.
	peers map[string][]string

	// serials is what each zone was published under.
	serials map[string]zoneSerial

	// served is what each service name was last written with, which is what
	// refresh compares against and takes back out; servedZone is the zone each
	// was written in, so a name that went is marked where it was.
	served     map[string]ecs_view.ViewAnswer
	servedZone map[string]string

	// names and zones are the working trees. A Tree.Insert is its own Txn and
	// Commit, so it copies every node on the path and allocates a channel for
	// each; one transaction per window reuses them instead.
	names *iradix.Txn[ecs_view.ViewAnswer]
	zones *iradix.Txn[*ecs_view.Zone]

	// v4 and v6 place a querier. They mutate in place, so publish hands over a
	// copy rather than these.
	v4, v6 critbit.Tree[ecs_view.ViewID]

	// fresh is a round having ended here since the stream was last lost: what
	// turns the view healthy, and the first time warm for good. lost is when
	// the stream last went, so a warm stamp older than that is not taken for one.
	fresh bool
	lost  time.Time

	// session is the newest stream session seen, and down that it closed (see
	// Plugin.sessionOf).
	session uint64
	down    bool

	// complete is a round having ended in this run: every zone holds the
	// whole fleet from then on, so whatever changes in one is a change a
	// secondary has to see, however the event that made it was stamped. What
	// a serial steps on, and the first time it pays what it owes.
	complete bool

	// compared is the first publish once complete having compared every zone
	// against the digest its serial was stored with.
	compared bool

	// live is the change being folded now coming from somewhere other than
	// the replay off the enricher's store: every zone it marks as moved is
	// marked live too (see zoneSerial.live). Set around each change by whoever
	// makes it, false otherwise.
	live bool

	// grown is every network a name has landed on that nothing was on: its
	// peering has not been read, since until now it could change no answer.
	// shrunk is a network having lost the last name on it, which may leave
	// the peering read so far covering every network there still is.
	grown  []string
	shrunk bool
}

// peerTargets is every network a read of the peering has to cover: each one a
// name is on, and each one the held peering names, so a read made before the
// replay has landed still re-reads what a restored peering holds. Sorted.
func (s *state) peerTargets() []string {
	seen := map[string]bool{}

	for key := range s.nets {
		seen[key] = true
	}

	for key, peers := range s.peers {
		seen[key] = true

		for _, peer := range peers {
			seen[peer] = true
		}
	}

	return slices.Sorted(maps.Keys(seen))
}

// newState builds the working copy a fold starts from, with the serials a
// previous run published under.
func newState(serials map[string]zoneSerial) *state {
	// A cold store that is disabled or empty loads nothing, and a zone entering
	// is a write: defaulted here rather than guarded at every one.
	if serials == nil {
		serials = map[string]zoneSerial{}
	}

	return &state{
		held:       map[string]*instance{},
		pending:    map[string]*instance{},
		nets:       map[string]map[ecs_view.ViewID]int{},
		serials:    serials,
		served:     map[string]ecs_view.ViewAnswer{},
		servedZone: map[string]string{},
		names:      iradix.New[ecs_view.ViewAnswer]().Txn(),
		zones:      iradix.New[*ecs_view.Zone]().Txn(),
	}
}

// write applies everything that has arrived, taking each instance from what the
// trees hold to what the last event about it said. An instance that moved and
// moved back inside one window compares equal here and reaches them not at all.
func (s *state) write(ttl uint32) bool {
	moved := false

	// confirmed is a restored replica read unchanged: its own records stay, but
	// a replica this run has read outranks restored ones in its service.
	confirmed := false

	// What was on a wire before is not new on it, nor gone from it, for
	// having been released and claimed again inside the write: an instance
	// changing its address on the only network it sits on leaves it empty for
	// the length of one apply.
	before := maps.Clone(s.nets)

	defer func() {
		s.grown = slices.DeleteFunc(s.grown, func(key string) bool {
			_, had := before[key]
			_, has := s.nets[key]

			return had || !has
		})

		for key := range before {
			_, has := s.nets[key]
			if !has {
				s.shrunk = true
			}
		}
	}()

	// live is any of it not being the replay: what refresh moves after it may
	// be its doing.
	live := false

	defer func() { s.live = false }()

	for key, next := range s.pending {
		delete(s.pending, key)

		prev := s.held[key]

		// Only updates are replayed: a delete is this run's.
		s.live = next == nil || !next.Restored()

		// The same records, so the trees are left alone - but the event held is
		// the newer one, or an instance restored from the cold store would read
		// as restored for ever once a live read confirmed it unchanged.
		if next != nil && prev != nil && next.Equal(prev.Event) {
			// Counted where the one it replaces was.
			next.aliasIn = prev.aliasIn
			s.held[key] = next

			if next.service != "" && prev.Restored() != next.Restored() {
				confirmed = true
				live = true
			}

			continue
		}

		if next == nil && prev == nil {
			continue
		}

		live = live || s.live

		s.apply(key, next, ttl)

		moved = true
	}

	// After every instance has moved: a service name is written from all of
	// its replicas at once.
	s.live = live

	if (moved || confirmed) && s.refresh(ttl) {
		moved = true
	}

	return moved
}

// arrive files what an event says of one instance for the next write, nil
// for one gone. Only the last word reaches the trees.
func (s *state) arrive(key string, inst *instance) {
	s.pending[key] = inst
}

// snapshot freezes the working copy. The radix trees commit by copy; the
// critbits mutate in place, so they are walked into fresh ones.
func (s *state) snapshot() *ecs_view.Snapshot {
	snap := &ecs_view.Snapshot{
		Tree:   s.names.Commit(),
		Denial: s.zones.Commit(),
	}

	for key, view := range s.v4.All() {
		snap.ByIPv4.Set(key, view)
	}

	for key, view := range s.v6.All() {
		snap.ByIPv6.Set(key, view)
	}

	// Committed, so the next patch opens a transaction of its own rather than
	// writing through nodes this snapshot is now holding.
	s.names = snap.Tree.Txn()
	s.zones = snap.Denial.Txn()

	return snap
}

// view is the set of networks an instance queries from, and the anonymous view
// of each of those networks on its own. Both are filed under every network in
// them, since that is how a name reaches the views that can see it.
//
// The views returned are the ones that did not exist before, which every name
// already on those wires still has to be written under.
func (s *state) view(inst *instance) (ecs_view.ViewID, []ecs_view.ViewID) {
	keys := netKeys(inst)

	own := ecs_view.ViewOf(keys)

	var fresh []ecs_view.ViewID

	if s.claim(keys, own) {
		fresh = append(fresh, own)
	}

	// A network is a view of its own, for a querier sitting on it that no
	// instance claims.
	for _, key := range keys {
		anon := ecs_view.ViewOf([]string{key})
		if s.claim([]string{key}, anon) && anon != own {
			fresh = append(fresh, anon)
		}
	}

	return own, fresh
}

// claim counts one view against every network in it, reporting whether it is
// one nothing had claimed yet.
func (s *state) claim(keys []string, id ecs_view.ViewID) bool {
	first := false

	for _, key := range keys {
		over := s.nets[key]
		if over == nil {
			over = map[ecs_view.ViewID]int{}
			s.nets[key] = over
			s.grown = append(s.grown, key)
		}

		if over[id] == 0 {
			first = true
		}

		over[id]++
	}

	return first
}

// release drops one instance's claim, and reports the views nothing is left in
// along with every network left with no view over it at all - the last instance
// leaving is what takes a view off the names on it, and its prefixes out of the
// address trees.
func (s *state) release(inst *instance) (freed []string, gone []ecs_view.ViewID) {
	keys := netKeys(inst)

	own := ecs_view.ViewOf(keys)

	for _, key := range keys {
		if s.drop(key, own) && !slices.Contains(gone, own) {
			gone = append(gone, own)
		}

		// Where the instance sits on one network its own view is that network's,
		// counted twice, and this second drop is the one that empties it: it is
		// gone all the same, and the names on that network's peers still hold it.
		anon := ecs_view.ViewOf([]string{key})
		if s.drop(key, anon) && !slices.Contains(gone, anon) {
			gone = append(gone, anon)
		}

		if len(s.nets[key]) == 0 {
			delete(s.nets, key)

			freed = append(freed, key)
		}
	}

	return freed, gone
}

// drop decrements one view over one network, reporting whether that was the
// last claim on it anywhere.
func (s *state) drop(key string, id ecs_view.ViewID) bool {
	over := s.nets[key]
	if over == nil {
		return false
	}

	over[id]--
	if over[id] > 0 {
		return false
	}

	delete(over, id)

	return true
}

// views is every view that can see one network, which is what a name on it has
// to be written under.
func (s *state) views(key string) []ecs_view.ViewID {
	out := make([]ecs_view.ViewID, 0, len(s.nets[key]))
	for id := range s.nets[key] {
		out = append(out, id)
	}

	return out
}

// peerViews is every view that sees one network through a peer rather than by
// sitting on it: the views over each network it is peered with, less the ones
// over it already. One hop only - a peer's peers are not looked at.
func (s *state) peerViews(key string) []ecs_view.ViewID {
	if len(s.peers[key]) == 0 {
		return nil
	}

	var out []ecs_view.ViewID

	seen := map[ecs_view.ViewID]struct{}{}

	for _, peer := range s.peers[key] {
		for id := range s.nets[peer] {
			_, direct := s.nets[key][id]
			if direct {
				continue
			}

			_, dup := seen[id]
			if dup {
				continue
			}

			seen[id] = struct{}{}

			out = append(out, id)
		}
	}

	return out
}

// peerKeys is the networks one view sees through its peers: each of its own
// networks' peers, less its own networks. One hop, as peerViews.
func (s *state) peerKeys(id ecs_view.ViewID) []string {
	if len(s.peers) == 0 {
		return nil
	}

	own := ecs_view.KeysOf(id)

	var out []string

	for _, key := range own {
		for _, peer := range s.peers[key] {
			if slices.Contains(own, peer) || slices.Contains(out, peer) {
				continue
			}

			out = append(out, peer)
		}
	}

	sort.Strings(out)

	return out
}

// setPeers swaps the peering the views see through. Every instance on a
// network whose peers changed is taken out under the old peering and written
// back under the new one. Nothing else can have moved: an instance on no
// changed network is seen by the same views as before, with the same
// addresses, since the views themselves are not the peering's.
//
// It reports whether the table moved, and whether that rewrote any name - a
// peering between networks nothing sits on yet changes no answer.
//
// No zone is marked moved: a peer half holds addresses its instance already
// answers with on its own network, so the zone a transfer carries - every
// view folded together - is the same set of records under any peering.
func (s *state) setPeers(next map[string][]string, ttl uint32) (changed, touched bool) {
	keys := changedPeers(s.peers, next)
	if len(keys) == 0 {
		// The same answers, but a peering held where none was: nil is no
		// peering read, which the cold store tells from one read empty.
		if s.peers == nil {
			s.peers = next
		}

		return false, false
	}

	changed = true

	affected := s.on(keys)

	// Two passes: every instance comes out under the peering it went in under
	// before any goes back under the new one, so one table holds at a time.
	for _, inst := range affected {
		for name, answer := range s.contributions(inst, ttl) {
			s.dropName(name, answer)
		}
	}

	s.peers = next

	for _, inst := range affected {
		for name, answer := range s.contributions(inst, ttl) {
			s.addName(name, answer)
		}
	}

	// Service names are not among the contributions: they are written from
	// every replica at once, and a replica's peer halves just moved.
	refreshed := s.refresh(ttl)

	return changed, len(affected) > 0 || refreshed
}

// changedPeers is every network whose peers differ between two tables, which
// is both ends of every pair that came or went.
func changedPeers(prev, next map[string][]string) []string {
	var out []string

	for key, peers := range prev {
		if !slices.Equal(peers, next[key]) {
			out = append(out, key)
		}
	}

	for key, peers := range next {
		_, had := prev[key]
		if !had && len(peers) > 0 {
			out = append(out, key)
		}
	}

	sort.Strings(out)

	return out
}
