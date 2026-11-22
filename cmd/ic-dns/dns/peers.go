package dns

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// peerRetryFirst and peerRetryLongest bound the backoff after a failed read. A
// read that fails is tried again by itself, so a peer event arriving while
// incusd hiccups is not left waiting for the next round.
const (
	peerRetryFirst   = time.Second
	peerRetryLongest = 30 * time.Second
)

// peerCallTimeout bounds each call of a read of the peering, not the read as a
// whole: a slow daemon (a network list taking 30 s on a loaded member was seen)
// makes the read long, never impossible, while a daemon not answering one call
// still fails it and leaves the held peering in place.
const peerCallTimeout = 30 * time.Second

// peerOwedGrace is how long a network new since warm may wait on a read of its
// peering with misses held at SERVFAIL. Past the longest backoff, so a read
// failing for a while is retried at least once inside it.
const peerOwedGrace = 45 * time.Second

// peerReader is what reading the peering takes off the Incus connection: the
// peers of one network, nothing else.
type peerReader interface {
	GetNetworkPeers(ctx context.Context, project string, network string) ([]incusapi.NetworkPeer, error)
}

// bounded is a peerReader whose every call has a deadline of its own.
type bounded struct {
	r       peerReader
	timeout time.Duration
}

func (b bounded) GetNetworkPeers(ctx context.Context, project string, network string) ([]incusapi.NetworkPeer, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	return b.r.GetNetworkPeers(ctx, project, network)
}

// peerActions are the lifecycle actions a peering moves on. None is enriched:
// the event only says to read the peering again.
var peerActions = []string{
	incusapi.EventLifecycleNetworkPeerCreated,
	incusapi.EventLifecycleNetworkPeerUpdated,
	incusapi.EventLifecycleNetworkPeerDeleted,
}

// readPeers is which of these networks are peered with which, by NetworkKey,
// sorted. The networks are the ones a name can be on - every network an
// instance this daemon serves sits on - by the key of the project that owns
// each; a peering between networks nothing sits on changes no answer, and the
// first instance on one asks for a read that takes it in.
//
// Only each network's own peer list is read: no project list, and no list of
// a project's networks, which in the default project brings the host's
// interfaces along and has Incus count what uses each of them (30 s against a
// loaded member). A pair counts only when both sides were read and both say
// Created, so a side outside these networks - in a project not served, or one
// nothing sits on - is never taken on trust.
//
// A side Incus refuses, no longer has, or that cannot peer at all (a bridge)
// is skipped. Anything else fails the whole read, so a daemon not answering
// cannot tear the peering down.
func readPeers(ctx context.Context, r peerReader, keys []string) (map[string][]string, error) {
	// Every peer each network names, by the key it names it with.
	says := map[string]map[string]bool{}

	for _, from := range keys {
		project, network, ok := strings.Cut(from, "/")
		if !ok {
			continue
		}

		peers, err := r.GetNetworkPeers(ctx, project, network)
		if err != nil {
			if refused(err) {
				continue
			}

			return nil, fmt.Errorf("listing peers of %s: %w", from, err)
		}

		for _, peer := range peers {
			to, ok := peerKey(peer)
			if !ok || to == from {
				continue
			}

			if says[from] == nil {
				says[from] = map[string]bool{}
			}

			says[from][to] = true
		}
	}

	out := map[string][]string{}

	for from, tos := range says {
		for to := range tos {
			// Both sides, or neither.
			if !says[to][from] {
				continue
			}

			out[from] = append(out[from], to)
		}
	}

	for key := range out {
		slices.Sort(out[key])
	}

	return out, nil
}

// peerKey is the network one peer points at, for a peer that is up and local.
// A peer towards an integration points outside Incus, at nothing a name can be
// on.
func peerKey(peer incusapi.NetworkPeer) (string, bool) {
	if peer.Status != incusapi.NetworkStatusCreated {
		return "", false
	}

	if peer.Type != "" && peer.Type != "local" {
		return "", false
	}

	if peer.TargetProject == "" || peer.TargetNetwork == "" {
		return "", false
	}

	return iutil.NetworkKey(peer.TargetProject, peer.TargetNetwork), true
}

// refused is Incus saying a side is not ours to read, not there any more, or
// of a type that cannot peer (400, "does not support peering").
func refused(err error) bool {
	return incusapi.StatusErrorCheck(err, http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound)
}

// peeringMoves says whether an event can have changed the peering: a peer
// itself, a network appearing, going or being renamed, or the stream coming
// back after events may have been missed.
func peeringMoves(ev *iutil.Event) bool {
	switch ev.Action() {
	case iutil.ActionConnected,
		incusapi.EventLifecycleNetworkCreated,
		incusapi.EventLifecycleNetworkDeleted,
		incusapi.EventLifecycleNetworkRenamed:
		return true
	}

	return slices.Contains(peerActions, ev.Action())
}

// describePeers is a peering as one line, for the log.
func describePeers(peers map[string][]string) string {
	var pairs []string

	for from, tos := range peers {
		for _, to := range tos {
			if from < to {
				pairs = append(pairs, from+"<->"+to)
			}
		}
	}

	slices.Sort(pairs)

	return strings.Join(pairs, " ")
}

// peerRead is one read of the peering, the networks it read, and the ask it
// answered.
type peerRead struct {
	peers map[string][]string
	keys  []string
	gen   uint64
}

// peerAsking is one ask for a read: the networks to read, and its number.
type peerAsking struct {
	keys []string
	gen  uint64
}

// askPeers asks for the peering to be read again, of every network a name is
// on now and every one the held peering names. One pending ask is enough, the
// newest: it names every network the older one did that still matters.
// moved says an event that can have changed the peering is why, so only a
// read asked for from here on makes it known. Run's goroutine alone, since it
// reads the state.
func (p *Plugin) askPeers(moved bool) {
	if !p.cfg.PeerViews {
		return
	}

	p.peerGen++

	if moved {
		p.peerNeed = p.peerGen
	}

	ask := peerAsking{keys: p.state.peerTargets(), gen: p.peerGen}

	select {
	case <-p.peerAsk:
	default:
	}

	p.peerAsk <- ask
}

// askIfGrew asks for a read where a write put a name on a network nothing was
// on: its peering was not read, since it could change no answer until now.
//
// Past the first read the view is warm, and a querier that sees a name on the
// new network only through a peer would be told it does not exist until that
// read lands: the network is owed a read, and a name a querier cannot see is
// SERVFAIL rather than NXDOMAIN while one is owed - for peerOwedGrace at most,
// each network from when it grew, so a read failing for good costs those
// queriers a while and no more.
func (p *Plugin) askIfGrew() {
	if len(p.state.grown) == 0 {
		return
	}

	grown := p.state.grown
	p.state.grown = nil

	p.askPeers(false)

	if p.peerReader == nil || !p.peersRead.Load() {
		return
	}

	by := time.Now().Add(p.peerOwedGrace)

	for _, key := range grown {
		p.peerOwed[key] = owedRead{gen: p.peerGen, by: by}
	}

	p.armOwed()
}

// askIfShrank takes back what a network losing its last name changes: it is
// owed no read any more, and the peering read so far may cover every network
// there still is, while the read being retried may be failing on the one that
// went - so the networks left are asked for afresh. It reports whether one
// shrank, for the caller to settle once what it moved is published.
func (p *Plugin) askIfShrank() bool {
	if !p.state.shrunk {
		return false
	}

	p.state.shrunk = false

	for key := range p.peerOwed {
		if p.state.nets[key] == nil {
			delete(p.peerOwed, key)
		}
	}

	p.armOwed()
	p.askPeers(false)

	return true
}

// owedRead is one network owed a read of its peering: the ask that has to
// cover it, and when it is given up on.
type owedRead struct {
	gen uint64
	by  time.Time
}

// armOwed sets the timer for the soonest network to be given up on, or none.
func (p *Plugin) armOwed() {
	p.peerOwedBy = nil

	var soonest time.Time

	for _, owed := range p.peerOwed {
		if soonest.IsZero() || owed.by.Before(soonest) {
			soonest = owed.by
		}
	}

	if !soonest.IsZero() {
		p.peerOwedBy = time.After(time.Until(soonest))
	}
}

// peeringKnown reports whether the peering held is one to turn warm on: one
// restored from the cold store, which counts until a read corrects it, or a
// read that landed and covered every network a name is on now, asked for
// after the last event that could have moved it. A slow or failing read never
// keeps a replica that restored its peering cold.
func (p *Plugin) peeringKnown() bool {
	return p.peersRestored || p.readKnown()
}

// readKnown is peeringKnown on the reads of this run alone.
func (p *Plugin) readKnown() bool {
	if p.peerCovered == nil || p.peerLanded < p.peerNeed {
		return false
	}

	for key := range p.state.nets {
		if !p.peerCovered[key] {
			return false
		}
	}

	return true
}

// settlePeering reports whether the peering lets the view turn warm, and
// latches the first time it does: until then a round waits on it.
func (p *Plugin) settlePeering() bool {
	if p.peersRead.Load() {
		return true
	}

	if !p.peeringKnown() {
		return false
	}

	p.peersRead.Store(true)

	if p.peerCovered == nil {
		p.logger.Info("warm on the network peering restored from the cold store, until a read lands")
	} else {
		p.logger.Info("the network peering has been read", "networks", len(p.peerCovered))
	}

	return true
}

// readPeerLoop reads the peering whenever asked, off the fold goroutine, and
// hands each read over to it. A failed read keeps what is held, says so, and
// is tried again after a backoff that doubles up to peerRetryLongest; an ask
// arriving meanwhile reads at once.
func (p *Plugin) readPeerLoop(ctx context.Context) {
	var (
		retry <-chan time.Time
		ask   peerAsking
	)

	backoff := p.peerRetry[0]

	for {
		select {
		case <-ctx.Done():
			return
		case ask = <-p.peerAsk:
		case <-retry:
		}

		retry = nil

		start := time.Now()

		keys := ask.keys

		peers, err := readPeers(ctx, bounded{r: p.peerReader, timeout: p.peerCallTimeout}, keys)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			p.logger.Warn("reading network peers, keeping what is held", "err", err, "retry", backoff)

			retry = time.After(backoff)
			backoff = min(backoff*2, p.peerRetry[1])

			continue
		}

		backoff = p.peerRetry[0]

		p.logger.Debug("read the network peering", "networks", len(keys), "took", time.Since(start))

		select {
		case p.peerIn <- peerRead{peers: peers, keys: keys, gen: ask.gen}:
		case <-ctx.Done():
			return
		}
	}
}

// peered takes one read of the peering on the fold goroutine, and publishes
// at once where it rewrote a name: a peering is not a burst worth waiting on.
//
// Never before the first publish: a read landing ahead of the replay off disk,
// or in the middle of it, would publish a fleet half replayed. The publish
// that folds the replay carries what this wrote. After it - a replay under the
// peering the cold store kept, corrected by the first read that lands - a
// change goes out at once, warm or not: a stale peer half is no better than a
// stale record, and the serials are safe either way, since a cold publish
// steps none and forgets none.
func (p *Plugin) peered(read peerRead) {
	peers := read.peers

	p.peerCovered = make(map[string]bool, len(read.keys))
	for _, key := range read.keys {
		p.peerCovered[key] = true
	}

	p.peerLanded = max(p.peerLanded, read.gen)

	// A read covering every network is what a round that ended before it was
	// waiting on, the same table as the one restored included.
	defer p.warmOnPeering()

	// A network owed a read is settled by one covering it asked for after it
	// grew and after the last event that can have moved the peering - an
	// older one may have been taken before the change that event announced -
	// or by nothing being on it any more.
	// Any network settled is one whose misses are final again, whatever is
	// still owed by the others: the snapshot carries the debt network by
	// network, and the timer left armed is only for the ones still owed.
	settled := false

	for key, o := range p.peerOwed {
		if (read.gen >= max(o.gen, p.peerNeed) && p.peerCovered[key]) || p.state.nets[key] == nil {
			delete(p.peerOwed, key)

			settled = true
		}
	}

	p.armOwed()

	// A read is this run's, never the replay's.
	p.state.live = true
	changed, touched := p.state.setPeers(peers, p.cfg.TTL)
	p.state.live = false

	if changed {
		p.logger.Info("network peering changed", "peers", describePeers(peers))
	}

	// What the next start restores as known is a table this run would turn
	// warm on, not merely the last one read: one too old for the last peer
	// event, or not covering every network a name is on, is no knowledge.
	// Compared with what is stored rather than with what was held: a read
	// too old to count may have put the table in memory already, and the one
	// confirming it changes nothing there while it is still to be stored.
	stored := false

	if p.readKnown() && (p.knownPeers == nil || len(changedPeers(p.knownPeers, p.state.peers)) > 0) {
		p.knownPeers = p.state.peers
		stored = true
	}

	switch {
	case changed && touched && p.published:
		// Whatever was pending goes out with it, so the snapshot is one fold.
		p.state.write(p.cfg.TTL)
		p.askIfGrew()
		p.askIfShrank()
		p.publish()

	case settled && p.published:
		// Nothing moved, but misses on the networks settled are final again,
		// which only a snapshot says.
		p.publish()

	case stored:
		// Nothing goes out, but the next start replays under this peering:
		// the serials stored with it are the ones last published, unchanged.
		// The first read finding nothing peered is a peering known all the
		// same, and the next start restores it as one.
		p.storeCold()
	}
}

// peerOwedOver gives up on the networks still owed a read of their peering:
// a name a querier cannot see is NXDOMAIN again, peer or no peer.
//
// Only those whose grace is over: each network has its own, from when it grew.
func (p *Plugin) peerOwedOver() {
	now := time.Now()
	over := 0

	for key, owed := range p.peerOwed {
		if owed.by.After(now) {
			continue
		}

		delete(p.peerOwed, key)

		over++
	}

	p.armOwed()

	if over == 0 {
		return
	}

	p.logger.Warn("the peering of networks new since warm could not be read, answering without it",
		"networks", over, "after", p.peerOwedGrace)

	if p.published {
		p.publish()
	}
}

// warmOnPeering turns the view warm where a round ended while the peering had
// not been read yet, now that it has - after what the read moved was
// published, so warm never answers from the table before it.
func (p *Plugin) warmOnPeering() {
	if p.peersRead.Load() {
		return
	}

	p.settle()
	p.announce()
}
