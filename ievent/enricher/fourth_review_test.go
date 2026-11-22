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

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/internal/testlib"
)

func TestFourthRunSessionAfterDrainingInbox(t *testing.T) {
	p := New(slog.Default(), InboxSize(1), SweepInterval(20*time.Millisecond), ReadDelay(0))
	f := &fixture{fleet: testlib.NewProject("p", 0, 0)}
	p.reads.fleet = f
	p.reads.readProject = f.GetProject
	commands := make(chan iutil.Command, 16)
	p.args.CommandOut = commands
	p.args.Next = func(*iutil.Event) {}
	control := func(action string, n uint64) *iutil.Event {
		return iutil.NewEvent(time.Now(), action, "", "", "").WithChainState(iutil.ChainCold).WithSession(n)
	}

	// The consumer has taken the first control, freeing one inbox slot.
	p.Handle(control(iutil.ActionDisconnected, 1))
	p.Handle(control(iutil.ActionConnected, 2))
	p.Handle(control(iutil.ActionDisconnected, 2))
	initial := <-p.inbox
	p.sweepSession = 1
	p.accept(t.Context(), initial)
	p.Handle(control(iutil.ActionConnected, 3))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	var sessions []uint64
	deadline := time.After(2 * time.Second)
	for len(sessions) < 4 {
		select {
		case cmd := <-commands:
			require.Equal(t, iutil.ActionSweepEnd, cmd.Action)
			sessions = append(sessions, cmd.Session)
		case <-deadline:
			t.Fatalf("missing round completions: %v", sessions)
		}
	}
	require.Equal(t, []uint64{3, 3}, sessions[2:], "actual sweeper completions: %v", sessions)
}

func TestFourthDisconnectAtCapacity(t *testing.T) {
	p := bare(t)
	p.inbox = make(chan *iutil.Event, 1)
	p.Handle(iutil.NewEvent(time.Now(), "occupied", "", "", ""))
	p.args.Next = func(ev *iutil.Event) {
		require.NoError(t, ev.Err(), "the downstream plugin received a dropped connection control")
	}
	p.Handle(iutil.NewEvent(time.Now(), iutil.ActionDisconnected, "", "", "").WithSession(1))

	// Kept with the controls, for Run to take behind what the inbox holds.
	held := p.held.take()
	require.Len(t, held, 1)
	require.Equal(t, iutil.ActionDisconnected, held[0].Action())
}

func TestFourthInventoryCheckpoint(t *testing.T) {
	for _, tt := range []struct {
		name string
		list string
		ends bool
	}{
		{"listed no projects", listedProjects, true},
		{"config read before any project listing", listedProjects, false},
		{"instance read before any instance listing", listedInstances, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := bare(t)
			if tt.ends {
				p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, at: time.Now()})
			} else if tt.list == listedProjects {
				p.state.setProject("p", map[string]string{})
			} else {
				inst := testlib.NewInstance("p", 0, 0)
				p.state.setInstance(&inst, nil)
			}
			data, err := json.Marshal(p.state.clone())
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "fleet.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			restored := New(slog.Default(), StoreFile(path))
			commands := make(chan iutil.Command, 1)
			restored.args.CommandOut = commands
			restored.brokenSince = time.Now().Add(-2 * restored.opts.roundGrace)
			restored.endBroken(t.Context(), &listingError{what: tt.list, project: "p", err: errors.New("unavailable")})
			require.Equal(t, tt.ends, len(commands) == 1, "checkpoint: %s", data)
		})
	}
}

func TestFourthEmptyInstanceListingReachesWriter(t *testing.T) {
	p := New(slog.Default(), StoreFile(filepath.Join(t.TempDir(), "fleet.json")))
	p.args.Next = func(*iutil.Event) {}
	p.state.setProject("p", map[string]string{})
	p.storeClone()
	data := <-p.storeIn
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	p.storeClone()
	select {
	case data = <-p.storeIn:
	default:
	}
	b, err := json.Marshal(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p.opts.StoreFile, b, 0600))
	again := New(slog.Default(), StoreFile(p.opts.StoreFile))
	commands := make(chan iutil.Command, 1)
	again.args.CommandOut = commands
	again.brokenSince = time.Now().Add(-2 * again.opts.roundGrace)
	again.endBroken(t.Context(), &listingError{what: listedInstances, project: "p", err: errors.New("unavailable")})
	require.Len(t, commands, 1, "stored immediately before the listing, and again at shutdown: %s", b)
}

func TestFourthNetworkHistoryAfterProjectPrune(t *testing.T) {
	p := bare(t)
	p.state.setProject("gone", nil)
	p.acceptNetwork(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleNetworkDeleted, "gone", "net", ""))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, at: time.Now()})
	require.Empty(t, p.reads.calls)
	require.Empty(t, p.state.projects)
	require.Empty(t, p.netMoved)
}

func TestFourthListingWhileHeldInstanceIsBeingReread(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	listing := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, nil, nil
	}
	p.reads.flush(t.Context())
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	read := awaitResult(t, p)
	p.acceptSweep(t.Context(), listing)
	p.settleRead(t.Context(), read)
	var actions []string
	for ev := range p.q.release() {
		actions = append(actions, ev.Action())
	}
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), awaitResult(t, p))
	for ev := range p.q.release() {
		actions = append(actions, ev.Action())
	}
	require.NotNil(t, p.state.instance("p", inst.Name))
	require.NotEmpty(t, actions)
	require.Equal(t, api.EventLifecycleInstanceUpdated, actions[len(actions)-1], "%v", actions)
}

func TestFourthReadSettlesAfterProjectExclusion(t *testing.T) {
	p := bare(t)
	fleet := testlib.NewProject("shop", 1, 0)
	p.opts.Project = func(*api.Project) bool { return false }
	msgs := make(chan sweepMsg, 1)
	_, _, err := sweepProjects(t.Context(), slog.Default(), sweepArgs{conn: &fixture{fleet: fleet}, serves: p.opts.Project, timeout: time.Second, sweepOut: msgs})
	require.NoError(t, err)
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &fleet.Instances[0], nil, nil
	}
	p.state.setProject("shop", map[string]string{})
	p.args.Wanted[api.EventLifecycleInstanceUpdated] = iutil.Want{Enrich: iutil.EnrichedInstanceWithInterfaces | iutil.EnrichedProject}
	p.reads.flush(t.Context())
	inst := fleet.Instances[0]
	p.accept(t.Context(), iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "shop", inst.Name, ""))
	read := awaitResult(t, p)
	p.acceptSweep(t.Context(), <-msgs)
	p.settleRead(t.Context(), read)
	for ev := range p.q.release() {
		require.False(t, ev.Instance() != nil && ev.Action() == api.EventLifecycleInstanceUpdated, "emitted %s/%s after exclusion", ev.ProjectName(), ev.Name())
	}
	require.Nil(t, p.state.instance("shop", inst.Name))
}

func TestFourthListingWhileOwedReadFails(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.reads.flush(t.Context())
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	errs := []error{api.StatusErrorf(404, "gone"), errors.New("read timeout")}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		err := errs[0]
		errs = errs[1:]
		return nil, nil, err
	}
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	first := awaitResult(t, p)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	second := awaitResult(t, p)
	require.Len(t, p.owedDelete, 1)
	var deleted bool
	p.args.Next = func(e *iutil.Event) {
		if e.Action() == api.EventLifecycleInstanceDeleted && e.Name() == inst.Name {
			deleted = true
		}
	}
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()})
	p.settleRead(t.Context(), second)
	for e := range p.q.release() {
		p.args.Next(e)
	}
	require.True(t, deleted, "the authoritative listing started after both reads and the last read has now failed")
}

func TestFourthLegacyInventoryDecoding(t *testing.T) {
	cases := []struct {
		name  string
		entry string
		ends  bool
	}{
		{"without instances", `{"config":{}}`, false},
		{"with one instance", `{"config":{},"instances":{"web":{"running":true}}}`, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(`{"version":1,"projects":{"p":` + tt.entry + `}}`)
			path := filepath.Join(t.TempDir(), "fleet.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			p := New(slog.Default(), StoreFile(path))
			commands := make(chan iutil.Command, 1)
			p.args.CommandOut = commands
			p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
			p.endBroken(t.Context(), &listingError{what: listedInstances, project: "p", err: errors.New("unavailable")})
			require.Equal(t, tt.ends, len(commands) == 1)
		})
	}
}

func TestFourthAbandonedColdReadIsRetried(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionProjects, names: []string{"p"}, at: time.Now()})
	p.settleRead(t.Context(), awaitResult(t, p))
	p.acceptSweep(t.Context(), sweepMsg{action: sweepActionInstances, project: "p", names: []string{inst.Name}, at: time.Now()})
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	p.args.CommandOut = make(chan iutil.Command, 1)
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	p.brokenSince = time.Now().Add(-2 * p.opts.roundGrace)
	p.endBroken(t.Context(), &listingError{what: listedProjects, err: errors.New("unavailable")})
	for ev := range p.q.release() {
		require.Error(t, ev.Err())
	}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		return &inst, nil, nil
	}
	require.NotEmpty(t, p.retries.due, "the abandoned read has no retry")
	p.fanOut(t.Context(), p.retries.take(time.Now().Add(time.Hour)))
	p.settleRead(t.Context(), awaitResult(t, p))
	var names []string
	for ev := range p.q.release() {
		require.NoError(t, ev.Err())
		names = append(names, ev.Name())
	}
	require.Equal(t, []string{inst.Name}, names)
}

func TestFourthOldListingAfterFailedRead(t *testing.T) {
	p := bare(t)
	inst := testlib.NewInstance("p", 0, 0)
	p.state.setInstance(&inst, nil)
	list := sweepMsg{action: sweepActionInstances, project: "p", at: time.Now()}
	p.args.Wanted = map[string]iutil.Want{api.EventLifecycleInstanceUpdated: {Enrich: iutil.EnrichedInstance}}
	errs := []error{api.StatusErrorf(404, "gone"), errors.New("unavailable")}
	p.reads.read = func(context.Context, string, string, bool, bool) (*api.Instance, *api.InstanceState, error) {
		err := errs[0]
		errs = errs[1:]
		return nil, nil, err
	}
	p.reads.flush(t.Context())
	ev := iutil.NewEvent(time.Now(), api.EventLifecycleInstanceUpdated, "p", inst.Name, "")
	p.accept(t.Context(), ev)
	first := awaitResult(t, p)
	p.accept(t.Context(), ev)
	p.settleRead(t.Context(), first)
	p.settleRead(t.Context(), awaitResult(t, p))
	require.Empty(t, p.reads.calls)
	p.acceptSweep(t.Context(), list)
	require.Len(t, p.owedDelete, 1, "an older listing decided the debt")
	list.at = time.Now()
	p.acceptSweep(t.Context(), list)
	require.Empty(t, p.owedDelete, "the newer listing decides")
}
