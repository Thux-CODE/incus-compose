package enricher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "github.com/lxc/incus/v7/shared/api"
	"github.com/stretchr/testify/require"

	"github.com/lxc/incus-compose/ievent/debounce"
	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

func TestFifthNewestListenerSurvivesUpstreamOverflow(t *testing.T) {
	p := New(slog.Default(), InboxSize(1), SweepInterval(15*time.Millisecond), ReadDelay(0))
	f := &fixture{fleet: testlib.NewProject("p", 0, 0)}
	p.reads.fleet = f
	p.reads.readProject = f.GetProject
	p.args.Next = func(*iutil.Event) {}
	ends := make(chan iutil.Command, 16)
	p.args.CommandOut = ends
	control := func(action string, session uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), action, "", "", "").WithChainState(iutil.ChainCold).WithSession(session)
	}
	p.sweepSession = 1
	p.accept(t.Context(), control(iutil.ActionDisconnected, 1))
	for ev := range p.q.release() {
		p.args.Next(ev)
	}
	p.Handle(control("backlog", 1))
	d := debounce.New(slog.Default(), debounce.InboxSize(2))
	delivered := make(chan *iutil.Event, 3)
	require.NoError(t, d.Setup(iutil.SetupArgs{Next: func(ev *iutil.Event) {
		p.Handle(ev)
		delivered <- ev
	}}))
	d.Handle(control(iutil.ActionConnected, 2))
	d.Handle(control(iutil.ActionDisconnected, 2))
	d.Handle(control(iutil.ActionConnected, 3))
	require.Equal(t, uint64(3), (<-delivered).Session())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	dDone := make(chan error, 1)
	go func() { dDone <- d.Run(ctx) }()
	defer func() { cancel(); <-dDone }()
	for range 2 {
		select {
		case ev := <-delivered:
			require.Equal(t, uint64(2), ev.Session())
		case <-ctx.Done():
			t.Fatal("debounce failed to drain")
		}
	}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	var sessions []uint64
	for len(sessions) < 4 {
		select {
		case cmd := <-ends:
			sessions = append(sessions, cmd.Session)
		case <-ctx.Done():
			t.Fatalf("missing round completions: %v", sessions)
		}
	}
	require.Equal(t, []uint64{3, 3}, sessions[2:], "round completions: %v", sessions)
}

func TestFifthGlobalListingWithUnreadProject(t *testing.T) {
	t.Skip("known issue: endBroken on a failed projects listing does not check that every project's instances were listed (N5)")
	p := bare(t)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"p"}, at: time.Now()})
	p.settleRead(t.Context(), awaitResult(t, p))
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	b, err := json.Marshal(p.state)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "fleet.json")
	require.NoError(t, os.WriteFile(path, b, 0600))
	reloaded := New(slog.Default(), StoreFile(path))
	commands := make(chan iutil.Command, 1)
	reloaded.args.CommandOut = commands
	reloaded.brokenSince = time.Now().Add(-2 * reloaded.opts.roundGrace)
	reloaded.endBroken(t.Context(), &listingError{what: listedProjects, err: errors.New("unavailable")})
	require.Empty(t, commands, "project p never finished its first instance listing")
}

func TestFifthFirstListingOfferedWithoutTicker(t *testing.T) {
	t.Skip("known issue: a listing bit set is written at the next store tick or at shutdown, not at once (N17)")
	p := New(slog.Default(), StoreFile(filepath.Join(t.TempDir(), "fleet.json")))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, at: time.Now()})
	require.NotEmpty(t, p.storeIn, "no snapshot offered after successful empty listing")
}

func TestFifthReadAcrossProjectReadmission(t *testing.T) {
	t.Skip("known issue: a read sent while its project was excluded is published if it lands after the project is served again (N6)")
	p := bare(t)
	f := testlib.NewProject("shop", 1, 1)
	inst := f.Instances[0]
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, f.States[inst.Name], nil
	}
	p.reads.flush(t.Context())
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, excluded: []string{"shop"}, at: time.Now()})
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "shop", inst.Name, "")
	p.accept(t.Context(), ev)
	if len(p.reads.calls) == 0 {
		return
	}
	read := awaitResult(t, p)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"shop"}, at: time.Now()})
	p.settleRead(t.Context(), awaitResult(t, p))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "shop", at: time.Now()})
	p.state.setNetwork(f.Networks[0])
	p.settleRead(t.Context(), read)
	for e := range p.q.release() {
		require.False(t, e.Instance() != nil, "published the instance of the excluded period after an empty listing")
	}
	require.Nil(t, p.state.instance("shop", inst.Name))
}

func TestFifthUnsentReadDoesNotConfirmOlderAbsence(t *testing.T) {
	t.Skip("known issue: a read not yet sent when a listing lands is taken as older than the listing (N8)")
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, errors.New("unavailable")
	}
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	// The list photographed the gap before a replacement under this name.
	// The instance read waits for the project's configuration.
	p.reads.flush(t.Context())
	p.reads.readProject = func(context.Context, string) (*api.Project, string, error) {
		return nil, "", errors.New("project temporarily unavailable")
	}
	p.reads.send(t.Context(), &call{key: resourceKey(kindProject, "p", ""), kind: kindProject, project: "p"})
	proj := awaitResult(t, p)
	list := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	p.accept(t.Context(), ev)
	p.acceptSweep(t.Context(), list)
	p.settleRead(t.Context(), proj)
	r := awaitResult(t, p)
	require.True(t, r.call.sent.After(list.at))
	p.settleRead(t.Context(), r)
	for e := range p.q.release() {
		require.NotEqual(t, api.EventLifecycleInstanceDeleted, e.Action())
	}
}

func TestFifthLongListingVerdictReachesSubscriber(t *testing.T) {
	t.Skip("known issue: the grace bounds when the end of a round is emitted, not when it leaves the line behind older reads (N16)")
	p := bare(t)
	const limit = 30 * time.Millisecond
	p.reads.timeout = limit
	p.opts.roundGrace = 5 * time.Millisecond
	p.reads.flush(t.Context())
	p.reads.read = func(ctx context.Context, _, _ string, _, _ bool) (*api.Instance, *api.InstanceState, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	for _, n := range []string{"other1", "other2", "other3", inst.Name} {
		p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", n, ""))
	}
	start := time.Now()
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: start})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	deadline := time.After(time.Second)
	for {
		select {
		case <-p.endGrace:
			p.endGrace = nil
			p.graceOver = true
			p.endIfRead(t.Context())
		case cmd := <-commands:
			require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
			p.accept(t.Context(), iutil.NewEvent(time.Now(), cmd.Action, "", "", "").WithChainState(cmd.ChainState))
		case r := <-p.reads.results:
			p.settleRead(t.Context(), r)
		case <-p.reads.timer.C:
			p.reads.retry(t.Context())
		case <-deadline:
			t.Fatal("the ordered end of round did not leave within the fixture deadline")
		}
		for ev := range p.q.release() {
			if ev.Action() == iutil.ActionSweepEnd {
				require.LessOrEqual(t, time.Since(start), limit+p.opts.roundGrace, "subscriber waited through the pool backlog")
				return
			}
		}
	}
}

func TestFifthInheritedNetworkAfterOwnerPrune(t *testing.T) {
	t.Skip("known issue: pruning an excluded owner project drops its network history, so a tenant's older read can put a deleted network back (N15)")
	p := bare(t)
	n := api.Network{Name: "bridge", Project: "default", Managed: true, Config: map[string]string{"ipv4.address": "10.22.0.1/24"}}
	p.reads.readNet = func(context.Context, string, string) (*api.Network, string, error) { return &n, "", nil }
	p.readNetwork(t.Context(), "tenant", "bridge")
	r := awaitResult(t, p)
	p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkDeleted, "default", "bridge", ""))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"tenant"}, excluded: []string{"default"}, at: time.Now()})
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionNetworks, project: "default", at: time.Now()})
	p.settleRead(t.Context(), r)
	_, _, exists := p.state.network("tenant", "bridge")
	require.False(t, exists, "tenant resolves the deleted owner's bridge")
}

func TestFifthFormatTwoOnRollback(t *testing.T) {
	b := []byte(`{"version":2,"projects_listed":true,"projects":{"p":{"config":{},"listed":true,"instances":{"web":{"running":true}}}}}`)
	path := filepath.Join(t.TempDir(), "fleet.json")
	require.NoError(t, os.WriteFile(path, b, 0600))
	p := New(slog.Default(), StoreFile(path))
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	var events []*iutil.Event
	p.args.Next = func(ev *iutil.Event) { events = append(events, ev) }
	p.replay()
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedProjects, err: errors.New("unavailable")})
	if storeVersion < 2 {
		err := os.WriteFile(path, []byte(`{"version":1,"projects":{"p":{"config":{},"listed":true,"instances":{"web":{"running":true}}}}}`), 0600)
		require.NoError(t, err)
		previous := New(slog.Default(), StoreFile(path))
		require.NotNil(t, previous.state.instance("p", "web"))
		previous.args.CommandOut = make(chan iutil.Command, 1)
		previous.brokenSince = time.Now().Add(-2 * previous.opts.roundGrace)
		previous.endBroken(t.Context(), &listingError{what: listedProjects, err: errors.New("unavailable")})
		require.Len(t, previous.args.CommandOut, 1)
		require.Empty(t, events)
		require.Empty(t, commands)
		return
	}
	require.Len(t, events, 1)
	require.Equal(t, uint64(0), events[0].Session())
	require.True(t, events[0].Restored())
	require.Equal(t, iutil.ChainCold, events[0].ChainState())
	require.Len(t, commands, 1)
}

func TestFifthSuccessfulReadPredatesActualDelete(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	state := testlib.NewInstanceState(0, 0)
	p.state.setNetwork(testlib.NewNetwork("p", 0))
	p.state.setInstance(&inst, state)
	p.reads.flush(t.Context())
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, state, nil
	}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	served := true
	p.args.Next = func(ev *iutil.Event) {
		if ev.Action() == api.EventLifecycleInstanceDeleted {
			served = false
		}
		if ev.Instance() != nil {
			served = addressed(ev.Instance())
		}
	}
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, ""))
	r := awaitResult(t, p)
	// Incus has already answered the GET. The delete happens before LIST,
	// while the worker's successful result waits to be folded.
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	p.settleRead(t.Context(), r)
	for ev := range p.q.release() {
		p.args.Next(ev)
	}
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionDone})
	require.Len(t, commands, 1)
	require.Empty(t, p.retries.due)
	require.False(t, served, "the round ended with a stale address and no retry")
}

func TestFifthUnknownMarkerAfterExclusion(t *testing.T) {
	t.Skip("known issue: a projects listing whose marker read timed out serves the project again instead of keeping its last exclusion (N7)")
	p := bare(t)
	p.opts.Project = func(*api.Project) bool { return false }
	ctx := t.Context()
	p.acceptSweep(ctx, sweepMsg{action: sweepActionProjects, excluded: []string{"shop"}, at: time.Now()})
	// GetProjectNames succeeded but GetProject("shop") timed out.
	// sweepProjects retains the name in served in this case.
	p.acceptSweep(ctx, sweepMsg{action: sweepActionProjects, names: []string{"shop"}, at: time.Now()})
	p.settleRead(ctx, awaitResult(t, p))
	f := testlib.NewProject("shop", 1, 1)
	inst := f.Instances[0]
	p.state.setNetwork(f.Networks[0])
	p.reads.flush(ctx)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, f.States[inst.Name], nil
	}
	p.accept(ctx, iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "shop", inst.Name, ""))
	if len(p.reads.calls) == 0 {
		return
	}
	p.settleRead(ctx, awaitResult(t, p))
	for e := range p.q.release() {
		require.False(t, e.Instance() != nil && addressed(e.Instance()), "a project whose marker still excludes it was advertised")
	}
}

func TestFifthExcludeProjectWithDeferredDelete(t *testing.T) {
	p := bare(t)
	ctx := t.Context()
	inst := testlib.NewInstance("shop", 0, 0)
	p.state.setNetwork(testlib.NewNetwork("shop", 0))
	p.state.setInstance(&inst, testlib.NewInstanceState(0, 0))
	p.reads.flush(ctx)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstanceWithInterfaces}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return nil, nil, api.StatusErrorf(404, "gone")
	}
	commands := make(chan iutil.Command, 1)
	p.args.CommandOut = commands
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "shop", inst.Name, "")
	p.accept(ctx, ev)
	first := awaitResult(t, p)
	p.accept(ctx, ev)
	p.settleRead(ctx, first)
	second := awaitResult(t, p)
	require.Len(t, p.owedDelete, 1)
	deleted := false
	p.args.Next = func(ev *iutil.Event) {
		if ev.Action() == api.EventLifecycleInstanceDeleted {
			deleted = true
		}
	}
	p.acceptSweep(ctx, sweepMsg{action: sweepActionProjects, excluded: []string{"shop"}, at: time.Now()})
	p.settleRead(ctx, second)
	for ev := range p.q.release() {
		p.args.Next(ev)
	}
	require.True(t, deleted, "two 404s and the owner project excluded, but no delete reached its subscriber")
}
