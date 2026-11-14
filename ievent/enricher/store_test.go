package enricher

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

// TestStoreSendNeverBlocks is the whole reason a clone crosses a channel: the
// fold may not wait on a disk, whatever the disk is doing. A clone the slot
// already held is dropped rather than queued, since the one behind it says
// everything it said.
func TestStoreSendNeverBlocks(t *testing.T) {
	t.Parallel()

	in := make(chan *state, 1)

	first := newState()
	first.setProject("first", map[string]string{})

	last := newState()
	last.setProject("last", map[string]string{})

	// Nothing is reading, so the slot fills and then has to be displaced.
	storeSend(in, first)
	storeSend(in, newState())
	storeSend(in, last)

	require.Len(t, in, 1, "the slot grew")

	got := <-in
	assert.NotNil(t, got.projectConfig("last"), "the newest clone was the one dropped")
}

// TestTheStoreReadsBackWhatItWrote is the other half of the cold store: what
// one run wrote is what the next one holds, down to an instance comparing equal
// to the read it came from.
func TestTheStoreReadsBackWhatItWrote(t *testing.T) {
	t.Parallel()

	fleet := testlib.NewProject("p", 2, 1)

	in := filled(t, fleet)
	in.setProject("p", map[string]string{"user.label.zone": "internal"})
	in.setProject("quiet", map[string]string{})

	b, err := json.Marshal(in)
	require.NoError(t, err)

	out, err := decodeState(b)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"user.label.zone": "internal"}, out.projectConfig("p"))
	assert.NotNil(t, out.projectConfig("quiet"), "a project that sets nothing came back as one never read")

	for i := range 2 {
		name := testlib.InstanceName(i)

		require.NotNil(t, out.instance("p", name), "%s did not come back", name)
		assert.True(t, in.instance("p", name).Equal(out.instance("p", name)),
			"%s came back as something else", name)
	}

	_, _, known := out.network("p", testlib.NetworkName(0))
	assert.True(t, known, "the network did not come back")

	assert.False(t, out.dirty, "a state just read back is already on disk")
}

// TestAStoreItCannotUseStartsCold: a cache that cannot be read is no cache, and
// a start without one is a start like the ones before it - never a crash.
func TestAStoreItCannotUseStartsCold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content *string
	}{
		{name: "no file yet"},
		{name: "not JSON at all", content: new("{\"projects\": {")},
		{name: "a version this binary does not know", content: new(`{"version": 99, "projects": {"p": {"config": {}}}}`)},
		{name: "the wrong shape", content: new(`{"projects": []}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "fleet.json")

			if tc.content != nil {
				require.NoError(t, os.WriteFile(path, []byte(*tc.content), 0o600))
			}

			got := loadState(slog.Default(), path, nil)

			require.NotNil(t, got)
			assert.Zero(t, got.projectCount(), "a store that could not be used put something in the state")
		})
	}
}

// TestAStoreFromBeforeTheVersionIsRead: v1.4.0 wrote the same shape with no
// version key, and that file is the one a first upgraded start finds.
func TestAStoreFromBeforeTheVersionIsRead(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "fleet.json")

	require.NoError(t, os.WriteFile(path, []byte(`{"projects": {"p": {"config": {"a": "1"}}}}`), 0o600))

	got := loadState(slog.Default(), path, nil)

	assert.Equal(t, map[string]string{"a": "1"}, got.projectConfig("p"))
}

// TestTheStoreKeepsOnlyWhatIsStillServed: --project or the marker may have
// changed between two starts, and a name the configuration of this one would
// never serve must not answer even until the first run.
func TestTheStoreKeepsOnlyWhatIsStillServed(t *testing.T) {
	t.Parallel()

	in := newState()

	for _, project := range []string{"shop", "blog"} {
		fleet := testlib.NewProject(project, 1, 1)

		storeNetworks(in, fleet.Networks)

		inst := fleet.Instances[0]
		held, _ := in.setInstance(&inst, fleet.States[inst.Name])
		require.NotNil(t, held)

		in.setProject(project, map[string]string{"user.dns": "true"})
	}

	// Held, but its configuration never read: nothing to ask the predicate.
	unread := testlib.NewProject("unread", 1, 1)
	storeNetworks(in, unread.Networks)

	inst := unread.Instances[0]
	held, _ := in.setInstance(&inst, unread.States[inst.Name])
	require.NotNil(t, held)

	b, err := json.Marshal(in)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "fleet.json")
	require.NoError(t, os.WriteFile(path, b, 0o600))

	got := loadState(slog.Default(), path, func(p *incusapi.Project) bool {
		return p.Name == "shop" && p.Config["user.dns"] == "true"
	})

	assert.NotNil(t, got.instance("shop", testlib.InstanceName(0)), "a served project lost its instances")

	assert.Nil(t, got.instance("blog", testlib.InstanceName(0)), "a project no longer served kept its instances")
	assert.Nil(t, got.projectConfig("blog"), "a project no longer served kept what it sets")

	assert.Nil(t, got.instance("unread", testlib.InstanceName(0)),
		"a project carrying no marker anyone could see kept its instances")

	// The wires stay: an instance in a served project may sit on one.
	_, _, known := got.network("blog", testlib.NetworkName(0))
	assert.True(t, known, "the networks of a project no longer served went with it")

	// A list of names needs no configuration to answer, so one never read is
	// still served by name.
	byName := loadState(slog.Default(), path, func(p *incusapi.Project) bool {
		return p.Name == "shop" || p.Name == "unread"
	})

	assert.NotNil(t, byName.instance("unread", testlib.InstanceName(0)),
		"a project served by name was dropped for a configuration nobody asked about")
}

// restartedFrom runs one plugin over a fleet until it has read it, stops it so
// it writes what it held, and starts a second over the same file: a restart.
func restartedFrom(t *testing.T, fleet *testlib.Project) *fixture {
	t.Helper()

	path := filepath.Join(t.TempDir(), "fleet.json")

	first := newFixture(t, StoreFile(path), StoreInterval(time.Hour))
	first.setFleet(fleet)
	first.collect(2 + len(fleet.Instances))
	first.stop()

	require.FileExists(t, path, "the first run left nothing for the second")

	return newFixture(t, StoreFile(path), StoreInterval(time.Hour))
}

// storeFleet is a project owning its networks, so a run reads them.
func storeFleet(instances int) *testlib.Project {
	fleet := testlib.NewProject("p", instances, 1)
	fleet.Project.Config[iutil.FeaturesNetworks] = "true"

	return fleet
}

// until takes events off the far end up to and including the first done
// accepts, failing the test if one is not along within next's timeout.
func (h *fixture) until(done func(*iutil.Event) bool) []*iutil.Event {
	h.t.Helper()

	var out []*iutil.Event

	for {
		ev := h.next()
		out = append(out, ev)

		if done(ev) {
			return out
		}
	}
}

// leftover is what reached the far end and nobody took, which after stop is
// everything the drain handed on.
func (h *fixture) leftover() []*iutil.Event {
	var out []*iutil.Event

	for len(h.out) > 0 {
		out = append(out, <-h.out)
	}

	return out
}

// about is every event in evs naming one instance.
func about(evs []*iutil.Event, project, instance string) []*iutil.Event {
	var out []*iutil.Event

	for _, ev := range evs {
		if ev.ProjectName() == project && ev.Name() == instance {
			out = append(out, ev)
		}
	}

	return out
}

// TestARestartServesWhatTheLastRunHeld is the point of the cold store: a start
// hands on what the last run held before Incus has answered anything, rather
// than nothing until the first run has been all the way round.
func TestARestartServesWhatTheLastRunHeld(t *testing.T) {
	t.Parallel()

	// No fleet: every listing refuses, so nothing below can have come from a
	// read in this run.
	h := restartedFrom(t, storeFleet(2))

	got := h.collect(2)

	for i, ev := range got {
		name := testlib.InstanceName(i)

		assert.Equal(t, name, ev.Name())
		assert.Equal(t, incusapi.EventLifecycleInstanceUpdated, ev.Action())
		assert.True(t, ev.Restored(), "a replayed instance read as a live one")
		assert.Equal(t, iutil.ChainCold, ev.ChainState(), "a replay claimed the fleet was read")
		assert.True(t, ev.Enriched(iutil.EnrichedInstance|iutil.EnrichedInstanceWithInterfaces|iutil.EnrichedProject),
			"a replayed instance went without what a read would have put on it")
		assert.True(t, ev.Instance().Running())
		assert.Zero(t, h.readsOf("p", name), "the replay cost a read")
	}
}

// TestTheFirstReadAfterARestartIsNewsOnce: the run's read of an instance that
// did not move says nothing about the instance, but it is the first fresh
// answer, and a consumer waiting on one has to see it. After that, an unchanged
// read goes no further, as before.
func TestTheFirstReadAfterARestartIsNewsOnce(t *testing.T) {
	t.Parallel()

	h := restartedFrom(t, storeFleet(1))
	name := testlib.InstanceName(0)

	restored := h.next()
	require.True(t, restored.Restored())

	// A read that landed, rather than one the drain below handed on unsettled.
	fresh := func(ev *iutil.Event) bool {
		return ev.Name() == name && ev.Enriched(iutil.EnrichedInstance) && !ev.Restored()
	}

	h.setFleet(storeFleet(1))

	// The round's end and its read of the instance, in whichever order.
	var seen []*iutil.Event

	for !slices.ContainsFunc(seen, fresh) || !slices.ContainsFunc(seen, isSweepEnd) {
		seen = append(seen, h.next())
	}

	got := seen[slices.IndexFunc(seen, fresh)]
	assert.True(t, restored.Equal(got), "the fleet did not move, and the read said it had")

	// A second round, at least as far as reading the instance again.
	reads := h.readsOf("p", name)

	h.send(iutil.ActionConnected, "", "")
	seen = append(seen, h.until(isSweepEnd)...)

	require.Eventually(t, func() bool { return h.readsOf("p", name) > reads },
		2*time.Second, time.Millisecond, "the second round never read the instance")

	h.stop()

	seen = append(seen, h.leftover()...)

	n := 0

	for _, ev := range seen {
		if fresh(ev) {
			n++
		}
	}

	assert.Equal(t, 1, n, "the first read after a restart was not handed on exactly once")
}

// isSweepEnd is the end of a round.
func isSweepEnd(ev *iutil.Event) bool { return ev.Action() == iutil.ActionSweepEnd }

// TestARunDeletesWhatWentWhileDown: an instance deleted while this process was
// down was announced to nobody, and the replay put it back. The run is the only
// thing that can take it out again: its listing, or a fan-out read finding it
// gone, whichever reaches it first.
func TestARunDeletesWhatWentWhileDown(t *testing.T) {
	t.Parallel()

	h := restartedFrom(t, storeFleet(2))
	gone := testlib.InstanceName(1)

	h.collect(2)

	h.mu.Lock()
	h.gone = map[string]bool{"p/" + gone: true}
	h.mu.Unlock()

	h.setFleet(storeFleet(1))

	h.until(func(ev *iutil.Event) bool {
		return ev.Name() == gone && ev.Action() == incusapi.EventLifecycleInstanceDeleted
	})

	h.stop()

	for _, ev := range about(h.leftover(), "p", gone) {
		assert.Equal(t, incusapi.EventLifecycleInstanceDeleted, ev.Action(),
			"an instance that went was said to exist after its delete")
	}

	assert.Nil(t, h.p.state.instance("p", gone), "the state still holds an instance that went")
	assert.NotNil(t, h.p.state.instance("p", testlib.InstanceName(0)))
}

// TestARunAnnouncesTheInstancesOfAProjectItDropped: a project the listing left
// out - gone, or no longer served - takes its instances with it, and each has
// to reach the chain as a delete, or a name restored from the store answers
// for ever.
func TestARunAnnouncesTheInstancesOfAProjectItDropped(t *testing.T) {
	t.Parallel()

	p := bare(t)

	var gone []string

	p.args.Next = func(ev *iutil.Event) {
		if ev.Action() == incusapi.EventLifecycleInstanceDeleted {
			gone = append(gone, ev.ProjectName()+"/"+ev.Name())
		}
	}

	p.state.setProject("shop", map[string]string{})
	p.state.setProject("blog", map[string]string{})
	p.state.setNetwork(testlib.NewNetwork("blog", 0))

	inst := testlib.NewInstance("blog", 0, 0)
	held, _ := p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	require.NotNil(t, held)

	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"shop"}})

	assert.Equal(t, []string{"blog/" + testlib.InstanceName(0)}, gone,
		"a project that went took its instances without saying so")
}
