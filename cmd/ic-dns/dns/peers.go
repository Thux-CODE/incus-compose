package dns

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	incusutil "github.com/lxc/incus/v7/shared/util"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// peerRetryFirst and peerRetryLongest bound the backoff after a failed read. A
// read that fails is tried again by itself, so a peer event arriving while
// incusd hiccups is not left waiting for the next round.
const (
	peerRetryFirst   = time.Second
	peerRetryLongest = 30 * time.Second
)

// peerReadTimeout bounds one read of the whole peering. A round is a handful
// of small reads per project, so this is only reached by a daemon not answering.
const peerReadTimeout = 30 * time.Second

// peerNetworkType is the only network type Incus peers.
const peerNetworkType = "ovn"

// peerReader is what reading the peering takes off the Incus connection.
type peerReader interface {
	GetProjects(ctx context.Context) ([]incusapi.Project, error)
	GetNetworks(ctx context.Context, project string) ([]incusapi.Network, error)
	GetNetworkPeers(ctx context.Context, project string, network string) ([]incusapi.NetworkPeer, error)
}

// peerActions are the lifecycle actions a peering moves on. None is enriched:
// the event only says to read the peering again.
var peerActions = []string{
	incusapi.EventLifecycleNetworkPeerCreated,
	incusapi.EventLifecycleNetworkPeerUpdated,
	incusapi.EventLifecycleNetworkPeerDeleted,
}

// readPeers is which served networks are peered with which, by NetworkKey,
// sorted. A pair counts only when both sides were read and both say Created:
// a side the certificate cannot read, or in a project this daemon does not
// serve, is never read, so its pairs are dropped rather than taken on trust.
//
// A side Incus refuses or no longer has is skipped. Anything else fails the
// whole read, so a daemon not answering cannot tear the peering down.
func readPeers(ctx context.Context, r peerReader, serves func(*incusapi.Project) bool) (map[string][]string, error) {
	projects, err := r.GetProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing projects: %w", err)
	}

	// Every peer each served network names, by the key it names it with.
	says := map[string]map[string]bool{}

	for i := range projects {
		project := &projects[i]

		if serves != nil && !serves(project) {
			continue
		}

		// A project without networks of its own shares the default project's,
		// which are read under the default project's own name.
		if project.Name != iutil.DefaultProject && !incusutil.IsTrue(project.Config[iutil.FeaturesNetworks]) {
			continue
		}

		networks, err := r.GetNetworks(ctx, project.Name)
		if err != nil {
			if refused(err) {
				continue
			}

			return nil, fmt.Errorf("listing networks of %s: %w", project.Name, err)
		}

		for _, network := range networks {
			if !network.Managed || network.Type != peerNetworkType {
				continue
			}

			peers, err := r.GetNetworkPeers(ctx, project.Name, network.Name)
			if err != nil {
				if refused(err) {
					continue
				}

				return nil, fmt.Errorf("listing peers of %s/%s: %w", project.Name, network.Name, err)
			}

			from := iutil.NetworkKey(project.Name, network.Name)

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

// refused is Incus saying a side is not ours to read, or not there any more.
func refused(err error) bool {
	return incusapi.StatusErrorCheck(err, http.StatusForbidden, http.StatusNotFound)
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

// askPeers asks for the peering to be read again. One pending ask is enough:
// a read starting after it sees whatever caused the second.
func (p *Plugin) askPeers() {
	if !p.cfg.PeerViews {
		return
	}

	select {
	case p.peerAsk <- struct{}{}:
	default:
	}
}

// readPeerLoop reads the peering whenever asked, off the fold goroutine, and
// hands each read over to it. A failed read keeps what is held, says so, and
// is tried again after a backoff that doubles up to peerRetryLongest; an ask
// arriving meanwhile reads at once.
func (p *Plugin) readPeerLoop(ctx context.Context) {
	var retry <-chan time.Time

	backoff := p.peerRetry[0]

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.peerAsk:
		case <-retry:
		}

		retry = nil

		readCtx, cancel := context.WithTimeout(ctx, peerReadTimeout)
		peers, err := readPeers(readCtx, p.peerReader, p.cfg.Project)

		cancel()

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

		select {
		case p.peerIn <- peers:
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
func (p *Plugin) peered(peers map[string][]string) {
	changed, touched := p.state.setPeers(peers, p.cfg.TTL)
	if !changed {
		return
	}

	p.logger.Info("network peering changed", "peers", describePeers(peers))

	if !touched || !p.published {
		// Nothing goes out, but the next start replays under this peering:
		// the serials stored with it are the ones last published, unchanged.
		p.storeCold()

		return
	}

	// Whatever was pending goes out with it, so the snapshot is one fold.
	p.state.write(p.cfg.TTL)
	p.publish()
}
