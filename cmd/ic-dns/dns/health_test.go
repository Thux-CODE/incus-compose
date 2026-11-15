package dns

import (
	"log/slog"
	"testing"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/cmd/ic-dns/dns/ecs_view"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"
)

// healthTest is a test the way compose writes it onto an instance.
const healthTest = `["CMD","true"]`

// replica is one replica of frontend on net-a. status is what ic-healthd last
// wrote on it; watched says whether it runs a check at all.
func replica(action, name, addr string, watched bool, status string) *iutil.Event {
	config := map[string]string{userLabel(labelServiceCompose): "frontend"}

	if watched {
		config[shared.HealthEnabledKey] = "true"
		config[shared.HealthKeyPrefix+"test"] = healthTest
	}

	if status != "" {
		config[shared.HealthStatusKey] = status
	}

	return event(action, "shop", name).
		WithInstance(read("shop", true, config, netsOf(map[string]string{"net-a": addr})), true)
}

func TestReadyNeedsAPassedCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config map[string]string
		want   bool
	}{
		{name: "nothing checks it", config: map[string]string{}, want: true},
		{
			name:   "a status without a check is not a verdict",
			config: map[string]string{shared.HealthStatusKey: shared.HealthStatusUnhealthy},
			want:   true,
		},
		{
			name: "a check that is not enabled",
			config: map[string]string{
				shared.HealthEnabledKey:         "false",
				shared.HealthKeyPrefix + "test": healthTest,
				shared.HealthStatusKey:          shared.HealthStatusStarting,
			},
			want: true,
		},
		{
			name: "compose's NONE is no check",
			config: map[string]string{
				shared.HealthEnabledKey:         "true",
				shared.HealthKeyPrefix + "test": `["NONE"]`,
			},
			want: true,
		},
	}

	for _, status := range []string{
		"", shared.HealthStatusUnknown, shared.HealthStatusStopped,
		shared.HealthStatusStarting, shared.HealthStatusUnhealthy, shared.HealthStatusHealthy, " healthy ",
	} {
		config := map[string]string{
			shared.HealthEnabledKey:         "true",
			shared.HealthKeyPrefix + "test": healthTest,
		}

		if status != "" {
			config[shared.HealthStatusKey] = status
		}

		tests = append(tests, struct {
			name   string
			config map[string]string
			want   bool
		}{name: "checked, status " + status, config: config, want: status == shared.HealthStatusHealthy || status == " healthy "})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, ready(read("shop", true, test.config, nil)))
		})
	}
}

func TestReadinessPicksBetweenReplicas(t *testing.T) {
	t.Parallel()

	const (
		started = incusapi.EventLifecycleInstanceStarted
		updated = incusapi.EventLifecycleInstanceUpdated
		deleted = incusapi.EventLifecycleInstanceDeleted
		healthy = shared.HealthStatusHealthy
	)

	tests := []struct {
		name string
		feed []*iutil.Event
		want []resolves
	}{
		{
			// A replica up but not yet passed is reachable at its own name, so
			// it can be checked and debugged, and sent no service traffic.
			name: "one ready of two is the only one answered, the other keeps its own name",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(started, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
				{qname: "frontend-2.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.6"}},
			},
		},
		{
			// Fail-open: readiness picks between replicas, it does not take a
			// service off the air - a stopped ic-healthd leaves every status
			// where it was.
			name: "none ready answers with all of them",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, shared.HealthStatusUnhealthy),
				replica(started, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5", "10.0.0.6"}},
			},
		},
		{
			name: "a single replica going unhealthy is still answered",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(updated, "frontend-1", "10.0.0.5", true, shared.HealthStatusUnhealthy),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
		{
			name: "a replica that went unhealthy leaves while another is ready",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(started, "frontend-2", "10.0.0.6", true, healthy),
				replica(updated, "frontend-2", "10.0.0.6", true, shared.HealthStatusUnhealthy),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
		{
			// What a new replica carries until its first check: nothing, or the
			// stopped ic-healthd wrote while it was created.
			name: "checked with no status or stopped is not ready",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(started, "frontend-2", "10.0.0.6", true, ""),
				replica(started, "frontend-3", "10.0.0.7", true, shared.HealthStatusStopped),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
		{
			name: "a replica nothing checks is ready",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", false, ""),
				replica(started, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5"}},
			},
		},
		{
			// The write ic-healthd makes is an instance-updated like any other,
			// and the read it triggers is what puts the replica in.
			name: "passing a check puts it in",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(started, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
				replica(updated, "frontend-2", "10.0.0.6", true, healthy),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.5", want: []string{"10.0.0.5", "10.0.0.6"}},
			},
		},
		{
			name: "the last ready one leaving lets the others answer",
			feed: []*iutil.Event{
				replica(started, "frontend-1", "10.0.0.5", true, healthy),
				replica(started, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
				event(deleted, "shop", "frontend-1"),
			},
			want: []resolves{
				{qname: "frontend.shop.example.", from: "10.0.0.6", want: []string{"10.0.0.6"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snap := fed(t, test.feed...).state.snapshot()

			for _, want := range test.want {
				assert.Equal(t, want.want, answered(t, snap, want.qname, want.from),
					"%s asked from %s", want.qname, want.from)
			}
		})
	}
}

// TestAServiceThatDidNotMoveStepsNoSerial: services rewrites from everything
// held, so a replica moving in one zone must not step another's serial.
func TestAServiceThatDidNotMoveStepsNoSerial(t *testing.T) {
	t.Parallel()

	p := fed(t,
		replica(incusapi.EventLifecycleInstanceStarted, "frontend-1", "10.0.0.5", true, shared.HealthStatusHealthy),
		eventOn(incusapi.EventLifecycleInstanceStarted, "blog", "api", map[string]string{metaService: "backend"},
			map[string]string{"net-a": "10.0.0.9"}))

	p.state.step(true, 0)

	before := p.state.serials["shop.example."].Serial

	// Warm, so a zone that moved does step.
	p.fold(warm(eventOn(incusapi.EventLifecycleInstanceUpdated, "blog", "api", map[string]string{metaService: "backend"},
		map[string]string{"net-a": "10.0.0.10"})))
	p.write()

	assert.Equal(t, before, p.state.serials["shop.example."].Serial, "shop did not move")
	assert.Equal(t, uint32(2), p.state.serials["blog.example."].Serial, "blog did")
}

func TestANotReadyReplicaKeepsItsOwnNameAndAliases(t *testing.T) {
	t.Parallel()

	config := ours(metaService, "frontend")
	config[userLabel(labelPrefix+metaAliases)] = "www"
	config[shared.HealthEnabledKey] = "true"
	config[shared.HealthKeyPrefix+"test"] = healthTest
	config[shared.HealthStatusKey] = shared.HealthStatusStarting

	inst := patchInstance(labeled("shop", "web", config, nil), "example.")
	require.NotNil(t, inst)

	assert.False(t, inst.ready)
	assert.Equal(t, "frontend.shop.example.", inst.service)
	assert.Equal(t, []string{"www.shop.example."}, inst.alias)

	s := newState(nil)
	s.apply("shop/web", inst, 5)
	s.refresh(5)

	snap := s.snapshot()

	for _, name := range []string{"web.shop.example.", "www.shop.example.", "frontend.shop.example."} {
		_, held := snap.Answers(name)
		assert.True(t, held, "%s is not answered", name)
	}
}

// watchedOn is one replica as held, the way patched builds them, carrying a
// health status and an enabled check.
func watchedOn(name, addr, status string, extra map[string]string) *instance {
	config := map[string]string{
		userLabel(labelServiceCompose):  "frontend",
		shared.HealthEnabledKey:         "true",
		shared.HealthKeyPrefix + "test": healthTest,
		shared.HealthStatusKey:          status,
	}

	for key, value := range extra {
		config[userLabel(labelPrefix+key)] = value
	}

	return patchInstance(event(incusapi.EventLifecycleInstanceUpdated, "shop", name).
		WithInstance(read("shop", true, config, netsOf(map[string]string{"net-a": addr})), true), "example.")
}

// TestAContestedAliasIsUntouchedByServices: an alias two instances claim is
// nobody's, whether or not it shares a name with a service, and the service
// name answers as it did before readiness existed.
func TestAContestedAliasIsUntouchedByServices(t *testing.T) {
	t.Parallel()

	s := newState(nil)

	s.apply("shop/web", watchedOn("web", "10.0.0.5", shared.HealthStatusHealthy, nil), 5)
	s.apply("shop/api", watchedOn("api", "10.0.0.6", shared.HealthStatusStarting, nil), 5)
	s.apply("shop/gate", instanceOn("shop", "gate", map[string]string{metaAliases: "www"}, map[string]string{"net-a": "10.0.0.7"}), 5)
	s.apply("shop/door", instanceOn("shop", "door", map[string]string{metaAliases: "www"}, map[string]string{"net-a": "10.0.0.8"}), 5)
	require.True(t, s.refresh(5))

	view := ecs_view.New(slog.Default())
	wire(newXFR(slog.Default(), nil), view, nil)

	view.Replace(s.snapshot())
	view.SetHealthy(true)

	a := &adapter{chain: view}

	w := &recorder{}
	a.ServeDNS(w, subnetQuery("www.shop.example.", "10.0.0.5"))
	assert.Equal(t, dns.RcodeNameError, w.msg.Rcode, "two claimants, so the alias is nobody's")

	w = &recorder{}
	a.ServeDNS(w, subnetQuery("frontend.shop.example.", "10.0.0.5"))
	require.Equal(t, dns.RcodeSuccess, w.msg.Rcode)
	require.Len(t, w.msg.Answer, 1, "the ready replica alone")
	assert.Equal(t, "10.0.0.5", w.msg.Answer[0].(*dns.A).A.String())
}

// TestTheSerialFollowsWhatIsServed: a replica going unhealthy and back moves
// the service name, so its zone steps each time; a refresh with nothing new
// steps nothing.
func TestTheSerialFollowsWhatIsServed(t *testing.T) {
	t.Parallel()

	const zone = "shop.example."

	s := newState(nil)

	s.apply("shop/web", watchedOn("web", "10.0.0.5", shared.HealthStatusHealthy, nil), 5)
	s.apply("shop/api", watchedOn("api", "10.0.0.6", shared.HealthStatusHealthy, nil), 5)
	s.refresh(5)
	s.step(true, 5)

	serial := func() uint32 { return s.serials[zone].Serial }
	born := serial()

	assert.False(t, s.refresh(5), "nothing moved, nothing rewritten")
	s.step(true, 5)
	assert.Equal(t, born, serial(), "a refresh that changed nothing steps nothing")

	s.apply("shop/api", watchedOn("api", "10.0.0.6", shared.HealthStatusUnhealthy, nil), 5)
	assert.True(t, s.refresh(5))
	s.step(true, 5)
	assert.Equal(t, born+1, serial(), "the service lost an address")

	s.apply("shop/api", watchedOn("api", "10.0.0.6", shared.HealthStatusHealthy, nil), 5)
	assert.True(t, s.refresh(5))
	s.step(true, 5)
	assert.Equal(t, born+2, serial(), "and got it back")
}

// TestASingleReplicaGoingUnhealthyStaysPublished: fail-open with one replica.
// Its record does not even change, so neither does the zone.
func TestASingleReplicaGoingUnhealthyStaysPublished(t *testing.T) {
	t.Parallel()

	const zone = "shop.example."

	s := newState(nil)

	s.apply("shop/web", watchedOn("web", "10.0.0.5", shared.HealthStatusHealthy, nil), 5)
	s.refresh(5)
	s.step(true, 5)

	born := s.serials[zone].Serial

	s.apply("shop/web", watchedOn("web", "10.0.0.5", shared.HealthStatusUnhealthy, nil), 5)
	s.refresh(5)

	assert.Equal(t, []string{"10.0.0.5"}, answered(t, s.snapshot(), "frontend."+zone, "10.0.0.5"))

	// The instance itself was patched, which marks its zone on its own: what
	// refresh adds is nothing, so a warm step is one step, not two.
	s.step(true, 5)
	assert.Equal(t, born+1, s.serials[zone].Serial)
}

// downReplica is a replica of frontend that is not running - stopped, or
// paused - read with the status ic-healthd last wrote, which may still be the
// previous run's healthy.
func downReplica(name string, watched bool, status string) *iutil.Event {
	config := map[string]string{userLabel(labelServiceCompose): "frontend"}

	if watched {
		config[shared.HealthEnabledKey] = "true"
		config[shared.HealthKeyPrefix+"test"] = healthTest
	}

	if status != "" {
		config[shared.HealthStatusKey] = status
	}

	return event(incusapi.EventLifecycleInstanceUpdated, "shop", name).
		WithInstance(read("shop", false, config, netsOf(map[string]string{"net-a": "10.0.0.9"})), true)
}

// TestAReplicaWithNoAddressIsNoCandidate: readiness picks among the replicas
// that have an address to give. One stopped or paused with a stale healthy, or
// one restored and read again down, would otherwise win and leave the service
// name with nothing in it while another replica is up.
func TestAReplicaWithNoAddressIsNoCandidate(t *testing.T) {
	t.Parallel()

	const service = "frontend.shop.example."

	t.Run("down with a stale healthy, the other starting", func(t *testing.T) {
		t.Parallel()

		snap := fed(t,
			replica(incusapi.EventLifecycleInstanceStarted, "frontend-2", "10.0.0.6", true, shared.HealthStatusStarting),
			downReplica("frontend-1", true, shared.HealthStatusHealthy),
		).state.snapshot()

		assert.Equal(t, []string{"10.0.0.6"}, answered(t, snap, service, "10.0.0.6"))
	})

	t.Run("a restored replica read again down does not take the restored service", func(t *testing.T) {
		t.Parallel()

		p := New(slog.Default(), Suffix("example"))
		p.next = func(_ *iutil.Event) {}

		p.fold(replica(incusapi.EventLifecycleInstanceUpdated, "frontend-1", "10.0.0.5", false, "").WithRestored())
		p.fold(downReplica("frontend-2", false, "").WithRestored())
		require.True(t, p.state.write(0))
		require.Equal(t, []string{"10.0.0.5"}, answered(t, p.state.snapshot(), service, "10.0.0.5"))

		// The first round reads the one that is down first, unchanged.
		p.fold(downReplica("frontend-2", false, ""))
		p.state.write(0)

		assert.Equal(t, []string{"10.0.0.5"}, answered(t, p.state.snapshot(), service, "10.0.0.5"))
	})

	t.Run("a sole replica with no lease yet leaves the name empty, not gone", func(t *testing.T) {
		t.Parallel()

		ev := event(incusapi.EventLifecycleInstanceStarted, "shop", "frontend-1").
			WithInstance(read("shop", true, map[string]string{userLabel(labelServiceCompose): "frontend"},
				map[string]onNet{"net-a": {project: iutil.DefaultProject, subnet: "10.0.0.0/24"}}), true)

		_, held := fed(t, ev).state.snapshot().Answers(service)
		assert.True(t, held)
	})
}

// TestReadinessIsDecidedViewByView: a service whose replicas sit on different
// networks answers each querier from the replicas it can reach. One ready
// replica elsewhere is no reason to leave a querier with none.
func TestReadinessIsDecidedViewByView(t *testing.T) {
	t.Parallel()

	on := func(name, net, addr, status string) *iutil.Event {
		config := map[string]string{
			userLabel(labelServiceCompose):  "frontend",
			shared.HealthEnabledKey:         "true",
			shared.HealthKeyPrefix + "test": healthTest,
			shared.HealthStatusKey:          status,
		}

		return event(incusapi.EventLifecycleInstanceStarted, "shop", name).
			WithInstance(read("shop", true, config, netsOf(map[string]string{net: addr})), true)
	}

	snap := fed(t,
		on("frontend-1", "net-a", "10.0.0.5", shared.HealthStatusHealthy),
		on("frontend-2", "net-a", "10.0.0.6", shared.HealthStatusStarting),
		on("frontend-3", "net-b", "10.0.1.6", shared.HealthStatusStarting),
	).state.snapshot()

	assert.Equal(t, []string{"10.0.0.5"}, answered(t, snap, "frontend.shop.example.", "10.0.0.6"),
		"net-a has a ready replica")
	assert.Equal(t, []string{"10.0.1.6"}, answered(t, snap, "frontend.shop.example.", "10.0.1.6"),
		"net-b reaches only the one starting")
}
