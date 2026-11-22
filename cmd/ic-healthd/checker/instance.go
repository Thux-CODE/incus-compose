package checker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/avast/retry-go/v5"

	"github.com/lxc/incus/v7/shared/util"

	"github.com/lxc/incus-compose/iclient"
	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"

	incusApi "github.com/lxc/incus/v7/shared/api"
)

// retryBusy runs write again while Incus rejects it for the instance's
// operation lock. The lock is taken by the driver, so a caller that creates an
// operation must do its wait inside write.
func retryBusy(ctx context.Context, conn *iclient.Connection, project string, name string, write func() error) error {
	return retry.New(
		retry.Context(ctx),
		retry.Attempts(6),
		retry.Delay(250*time.Millisecond),
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			return errors.Is(err, iclient.ErrInstanceBusy)
		}),
	).Do(func() error {
		err := write()
		if !errors.Is(err, iclient.ErrInstanceBusy) {
			return err
		}

		// Start the next attempt when the instance is free rather than on a
		// fixed delay that may well land while the lock is still held.
		waitErr := conn.WaitInstanceBusy(ctx, project, name)
		if waitErr != nil {
			return waitErr
		}

		return err
	})
}

func patchInstanceConfig(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, instance string, config map[string]string) error {
	// Bounded because this runs on the Run loop.
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	logger.Log(ctx, shared.LevelTrace, "Updating an instances config", "instance", instance, "patch", config)

	return retryBusy(ctx, conn, project, instance, func() error {
		return conn.PatchInstanceConfig(ctx, project, instance, config)
	})
}

// WriteStatus writes one health status value onto one instance.
func WriteStatus(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, instance string, status string) error {
	return patchInstanceConfig(ctx, logger, conn, project, instance, map[string]string{shared.HealthStatusKey: status})
}

type instanceConfig struct {
	test          []string
	startPeriod   time.Duration
	startInterval time.Duration
	interval      time.Duration
	timeout       time.Duration
	retries       int

	// The image's own process, from oci.cwd/oci.uid/oci.gid. A check is
	// written for that process and an exec is none of it by default.
	cwd string
	uid uint32
	gid uint32

	restart string

	// stoppedOnPurpose is user.healthcheck.stopped as read: a stop that
	// incus-compose stop or pause asked for, which no policy undoes.
	stoppedOnPurpose bool

	running bool
}

// restartOwed says a stop read on the instance calls for a restart: it has a
// restart policy and was not stopped on purpose.
func (ic *instanceConfig) restartOwed() bool {
	return slices.Contains(shared.RestartPolicies, ic.restart) && !ic.stoppedOnPurpose
}

// command is what to run in an instance, and as what.
type command struct {
	cmd []string
	cwd string
	uid uint32
	gid uint32
}

func (ic *instanceConfig) equals(other *instanceConfig) bool {
	return slices.Equal(ic.test, other.test) &&
		ic.startPeriod == other.startPeriod &&
		ic.startInterval == other.startInterval &&
		ic.interval == other.interval &&
		ic.timeout == other.timeout &&
		ic.retries == other.retries &&
		ic.cwd == other.cwd &&
		ic.uid == other.uid &&
		ic.gid == other.gid &&
		ic.restart == other.restart &&
		ic.stoppedOnPurpose == other.stoppedOnPurpose &&
		ic.running == other.running
}

type instanceResult struct {
	kind    instanceResultKind
	name    string
	project string

	// ctx identifies the action that ran: an abandoned one can still deliver,
	// and the loop compares this against the context it holds. Unset by a
	// status write.
	ctx context.Context

	config *instanceConfig

	// status is the value that was written (instanceResultStatus), and run the
	// instance's run when the write was sent.
	status string
	run    uint64

	// write identifies a status write, as ctx does an action: the key is the
	// project and the name, and an instance deleted and created again under
	// them gets the result of the write its predecessor sent.
	write *statusWrite

	// alreadyRunning says a restart owed for a stop found the instance
	// running and touched nothing (instanceResultRestarted).
	alreadyRunning bool

	err error
}

// statusWrite is one status write's identity. Never zero-sized: pointers to
// distinct zero-size values may compare equal.
type statusWrite struct {
	status string
}

// reportUnwatched says why an event's read is not watched. It runs on the loop
// itself and never sends a result: the loop is the only reader of results, and
// with the channel full a send from it would wait on itself for good.
func reportUnwatched(logger *slog.Logger, project string, name string, err error) {
	switch {
	case errors.Is(err, ErrInstanceNotEnabled):
		// The one case worth saying out loud: it asked to be checked and
		// will not be.
		logger.Warn("Not watching an instance that has a healthcheck but is not enabled",
			"project", project, "instance", name, "key", shared.HealthEnabledKey)
	case errors.Is(err, ErrInstanceNoHealthcheck):
		// Nothing to say: these are the normal reasons to skip.
	default:
		logger.Error("Not watching an instance",
			"project", project, "instance", name, "error", err)
	}
}

func (r instanceResult) Key() string {
	return r.project + "/" + r.name
}

type instance struct {
	name    string
	project string
	config  *instanceConfig

	state instanceState

	due    time.Time
	action instanceAction

	// actionContext is created fresh per action and cleared the moment one
	// ends, so its identity says which action a result came from.
	actionDeadline time.Time
	actionContext  context.Context
	actionCancel   context.CancelFunc

	inRestart   bool
	restartDone time.Time

	// run counts the starts seen, and actionRun is the run a check was sent
	// for. A start can land while a check is out; that check probed the run
	// before it, so its verdict says nothing about this one.
	run       uint64
	actionRun uint64

	// ran says the daemon has seen the instance running. One it has only
	// ever seen stopped - found so at discovery, say - waits for its start
	// event whatever its policy, as it did before the daemon came up.
	ran bool

	// unproven says this run, opened by a start, has no healthy of its own on
	// the instance yet. Until one lands, every pass writes healthy even when
	// status already says so: status may come from a read taken before the
	// daemon's last write landed, which still carried the last run's healthy -
	// or before a healthy write that then failed.
	unproven bool

	// failures counts consecutive failed checks since the last success.
	// Failures inside the start period do not count, as in docker.
	failures int

	// status is the last value known to be on the instance, so a write only
	// happens on a transition. Discovery refreshes it.
	status string

	// writing says a status write is in flight. One at a time per instance:
	// two racing writes land in any order, and the one that loses would stay
	// on the instance while status says the other.
	writing bool

	// write is the write in flight, which only its own result releases.
	write *statusWrite

	// queued is the status asked for while a write was in flight, written
	// once it lands.
	queued string

	// queuedRun is the run queued was asked for in. Sent later, it still
	// belongs to that run: a healthy queued before a start proves nothing
	// about the run that follows.
	queuedRun uint64

	// restartDelay doubles per restart; a healthy check rebases it.
	restartDelay time.Duration

	// restartStops says what the restart owed is for. True: an unhealthy run,
	// which the restart stops first even though it is running. False: a stop,
	// so a read showing it running again cancels the restart, and a worker
	// finding it running starts nothing and stops nothing.
	restartStops bool
}

// instanceStarted puts an instance in the shape a fresh start leaves it in: due
// for a check at once, its failure run cleared and its start period re-armed.
func instanceStarted(inst *instance, now time.Time) {
	newRun(inst, now)

	inst.state = instanceIdle
	inst.action = instanceActionCheck
	inst.restartStops = false
	inst.due = now
}

// newRun is what a start does to the run itself, whether or not an action is
// in flight: a new run number, its failure run cleared and its start period
// re-armed. The action and its schedule are the caller's.
func newRun(inst *instance, now time.Time) {
	inst.config.running = true
	inst.ran = true
	inst.run++
	inst.unproven = true
	inst.failures = 0

	// As in docker, a restarted instance gets its start period back.
	inst.inRestart = inst.config.startPeriod > 0
	if inst.inRestart {
		inst.restartDone = now.Add(inst.config.startPeriod)
	}
}

// baseRestartDelay is interval*retries, clamped to [defaultRestartDelay, maxRestartDelay].
func baseRestartDelay(cfg *instanceConfig) time.Duration {
	if cfg.interval <= 0 || cfg.retries <= 0 {
		return defaultRestartDelay
	}

	return max(min(cfg.interval*time.Duration(cfg.retries), maxRestartDelay), defaultRestartDelay)
}

func getInstance(ctx context.Context, conn *iclient.Connection, project string, name string) (*incusApi.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	inst, _, err := conn.GetInstance(ctx, project, name, nil)
	if err != nil {
		return nil, err
	}

	return &inst.Instance, nil
}

func parseInstanceConfig(config map[string]string, running bool) (*instanceConfig, error) {
	wantsChecking := config[shared.HealthKeyPrefix+"test"] != "" ||
		slices.Contains(shared.RestartPolicies, config[shared.HealthKeyPrefix+"restart"])

	// Watching is opt-in. One that looks like it wants checking but never
	// opted in is reported rather than assumed.
	if !util.IsTrue(config[shared.HealthEnabledKey]) {
		if wantsChecking {
			return nil, ErrInstanceNotEnabled
		}

		return nil, ErrInstanceNoHealthcheck
	}

	if !wantsChecking {
		return nil, ErrInstanceNoHealthcheck
	}

	cfg := instanceConfig{
		startPeriod:   defaultRestartPeriod,
		startInterval: defaultRestartInterval,
		interval:      defaultInterval,
		timeout:       defaultTimeout,
		retries:       defaultRetries,
		cwd:           config["oci.cwd"],
		restart:       config[shared.HealthKeyPrefix+"restart"],

		stoppedOnPurpose: util.IsTrue(config[shared.HealthStoppedKey]),
		running:          running,
	}

	// Absent parses to 0, which is root, which is where an exec landed anyway.
	uid, err := strconv.ParseUint(config["oci.uid"], 10, 32)
	if err == nil {
		cfg.uid = uint32(uid)
	}

	gid, err := strconv.ParseUint(config["oci.gid"], 10, 32)
	if err == nil {
		cfg.gid = uint32(gid)
	}

	testRaw := config[shared.HealthKeyPrefix+"test"]
	if testRaw == "" && slices.Contains(shared.RestartPolicies, cfg.restart) {
		// Restart policy without a test: probe with a no-op so run state is watched.
		testRaw = `["NONE"]`
	}

	if testRaw != "" {
		if err := json.Unmarshal([]byte(testRaw), &cfg.test); err != nil {
			return nil, fmt.Errorf("parsing test: %w", err)
		}
	}

	if len(cfg.test) > 0 && cfg.test[0] == "CMD-SHELL" && len(cfg.test) < 2 {
		return nil, errors.New("CMD-SHELL requires a command")
	}

	v := config[shared.HealthKeyPrefix+"start_period"]
	if v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parsing start_period: %w", err)
		}
		cfg.startPeriod = d
	}

	v = config[shared.HealthKeyPrefix+"start_interval"]
	if v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parsing start_interval: %w", err)
		}
		if d <= 0 {
			return nil, errors.New("start_interval must be greater than 0")
		}
		cfg.startInterval = d
	}

	v = config[shared.HealthKeyPrefix+"interval"]
	if v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parsing interval: %w", err)
		}
		if d <= 0 {
			return nil, errors.New("interval must be greater than 0")
		}
		cfg.interval = d
	}

	v = config[shared.HealthKeyPrefix+"timeout"]
	if v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parsing timeout: %w", err)
		}
		if d <= 0 {
			return nil, errors.New("timeout must be greater than 0")
		}
		cfg.timeout = d
	}

	v = config[shared.HealthKeyPrefix+"retries"]
	if v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing retries: %w", err)
		}
		if n == 0 {
			return nil, errors.New("retries must be greater than 0")
		}
		cfg.retries = int(n)
	}

	return &cfg, nil
}

// unparkForRestart schedules the restart a stop calls for, one backoff away,
// widening it as a crash loop would. A check out probed the run that ended:
// it is called off, so its result cannot schedule the same restart again.
func unparkForRestart(inst *instance, now time.Time) {
	if inst.state == instanceChecking && inst.actionCancel != nil {
		inst.actionCancel()
		inst.actionCancel = nil
		inst.actionContext = nil
		inst.actionDeadline = time.Time{}
	}

	inst.state = instanceIdle
	inst.action = instanceActionRestart
	inst.restartStops = false
	inst.due = now.Add(inst.restartDelay)
	inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)
}

// instKey is how an instance is held: by project and name, never by name
// alone. A delete is never enriched, so the key cannot ask for a read.
func instKey(ev *iutil.Event) string {
	return ev.ProjectName() + "/" + ev.Name()
}

// saveInstance folds one enriched event into what is held, leaving a new
// instance the way a discovery does: idle, due for a check at once.
//
// It keeps the restart invariant, both ways: an instance seen running, read
// stopped, with a restart policy and no marker, has a restart in flight, due
// or refused; one read running owes no restart for a stop.
func saveInstance(instances map[string]*instance, ev *iutil.Event) (*instance, error) {
	if !ev.Enriched(iutil.EnrichedInstance) {
		return nil, errors.New("event without an instance read")
	}

	k := instKey(ev)
	evInst := ev.Instance()

	instConfig, err := parseInstanceConfig(maps.Collect(evInst.Config()), evInst.Running())
	if err != nil {
		delete(instances, k)
		return nil, err
	}

	inst, ok := instances[k]
	if !ok {
		now := time.Now()

		inst = &instance{
			name:         ev.Name(),
			project:      ev.ProjectName(),
			state:        instanceIdle,
			action:       instanceActionCheck,
			due:          now,
			restartDelay: baseRestartDelay(instConfig),
		}

		if !instConfig.running {
			inst.state = instanceParked
			inst.action = ""
		} else {
			inst.ran = true

			if instConfig.startPeriod > 0 {
				inst.inRestart = true
				inst.restartDone = now.Add(instConfig.startPeriod)
			}
		}

		instances[k] = inst
	} else if !instConfig.running {
		switch {
		case inst.state == instanceRestarting:
			// The restart in flight owns it, and starts it anyway.
		case inst.ran && instConfig.restartOwed():
			// A stop known from a read alone: its event lost, folded while a
			// restart was out whose start reported since, or one whose
			// marker has been taken off while it stays stopped. Parked, or
			// refused for a marker no longer there, nothing would bring it
			// back. A restart already due keeps its backoff.
			if inst.action != instanceActionRestart || inst.state == instanceParked {
				unparkForRestart(inst, time.Now())
			}
		case inst.action == instanceActionCheck:
			// Stopped on purpose, without a policy, or never seen running:
			// its start event is what brings it back.
			inst.state = instanceParked
			inst.action = ""
		}
	} else if instConfig.running && inst.state == instanceParked {
		instanceStarted(inst, time.Now())
	} else if instConfig.running && inst.state == instanceIdle && inst.action == instanceActionRestart && !inst.restartStops {
		// Running again, its start unseen, before the restart a stop made
		// due went out: the stop is over, and firing the restart now would
		// stop a working instance. An unhealthy run's restart still goes.
		instanceStarted(inst, time.Now())
	}

	// While a write is out, status is what was asked for and the read may
	// predate it: adopting it would make the write behind look landed. Its
	// result, and the next read, settle it.
	if !inst.writing {
		s, ok := evInst.ConfigValue(shared.HealthStatusKey)
		if ok {
			inst.status = s
		} else {
			inst.status = shared.HealthStatusUnknown
		}
	}

	inst.config = instConfig

	return inst, nil
}

// handleInstanceEvent does what the name says, all operations MUST be non-blocking.
// goroutines are not allowed to modify instances or its values.
func handleInstanceEvent(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, instances map[string]*instance, results chan instanceResult, ev *iutil.Event) {
	// Everything below keys on the name; an action carrying none would fold
	// into an entry called "".
	if ev.Name() == "" {
		return
	}

	switch ev.Action() {
	case incusApi.EventLifecycleInstanceStarted, incusApi.EventLifecycleInstanceRestarted, incusApi.EventLifecycleInstanceResumed:
		inst, err := saveInstance(instances, ev)
		if err != nil {
			reportUnwatched(logger, ev.ProjectName(), ev.Name(), err)
			return
		}

		// An action in flight owns the instance; its result says what comes
		// next. A restart writes starting itself once it lands; a check does
		// not, and whatever verdict was on the instance is the last run's, so
		// the new run begins now and starting goes with it. The check out
		// probed the run before, so its result only frees the slot.
		if inst.state == instanceChecking || inst.state == instanceRestarting {
			if inst.state == instanceChecking {
				newRun(inst, time.Now())
				reportStarting(ctx, logger, conn, ev.ProjectName(), results, inst)
			}

			return
		}

		// It is running again, whoever did it, so a queued restart is moot.
		instanceStarted(inst, time.Now())
		reportStarting(ctx, logger, conn, ev.ProjectName(), results, inst)

	case incusApi.EventLifecycleInstanceStopped, incusApi.EventLifecycleInstanceShutdown:
		// Before the fold, which parks an instance read stopped.
		prev, ok := instances[instKey(ev)]
		checking := ok && prev.state == instanceChecking

		inst, err := saveInstance(instances, ev)
		if err != nil {
			reportUnwatched(logger, ev.ProjectName(), ev.Name(), err)
			return
		}

		policy := slices.Contains(shared.RestartPolicies, inst.config.restart)

		// A restart in flight owns the instance: this is its own stop, or one
		// it ends anyway by starting the instance, and its result writes
		// starting. Taking the stop for a new one would fire a second restart
		// beside it, and a stopped written now would only land once the
		// start lets go of the instance, on a running one.
		if inst.state == instanceRestarting && policy {
			return
		}

		// Before the branches below, one of which stops watching it entirely.
		reportStatus(ctx, logger, conn, ev.ProjectName(), results, inst, shared.HealthStatusStopped)

		// Before anything else, so a policy dropped since discovery still drops
		// the instance rather than scheduling a restart for it.
		if !policy {
			if inst.actionCancel != nil {
				inst.actionCancel()
			}

			delete(instances, instKey(ev))

			return
		}

		// A check out probed a run that has just ended; its result would
		// schedule the restart below a second time and widen the backoff
		// twice. The watchdog has nothing left to reap.
		if checking && inst.actionCancel != nil {
			inst.actionCancel()
			inst.actionCancel = nil
			inst.actionContext = nil
			inst.actionDeadline = time.Time{}
		}

		// Already waiting on a restart: leave its backoff alone.
		if inst.action == instanceActionRestart {
			inst.state = instanceIdle
			return
		}

		// A stop nobody asked for, so widen the window: a crash loop backs off.
		inst.state = instanceIdle
		inst.action = instanceActionRestart
		inst.restartStops = false
		inst.due = time.Now().Add(inst.restartDelay)
		inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)

	case incusApi.EventLifecycleInstanceUpdated:
		var before uint64
		if prev, ok := instances[instKey(ev)]; ok {
			before = prev.run
		}

		inst, err := saveInstance(instances, ev)
		if err != nil {
			reportUnwatched(logger, ev.ProjectName(), ev.Name(), err)
			return
		}

		// A stopped instance has no verdict to earn until it runs again, and a
		// sweep's trickle is the first one to say so.
		if !inst.config.running {
			reportStatus(ctx, logger, conn, ev.ProjectName(), results, inst, shared.HealthStatusStopped)
		}

		// The read found it running again, its start unseen: a new run, with
		// no verdict yet, as after a start event.
		if inst.run != before && inst.config.running {
			reportStarting(ctx, logger, conn, ev.ProjectName(), results, inst)
		}
	case incusApi.EventLifecycleInstanceDeleted:
		k := instKey(ev)

		inst, ok := instances[k]
		if ok && inst.actionCancel != nil {
			inst.actionCancel()
		}

		delete(instances, k)
	default:
		// The chain walks more than this plugin asked for; everything else
		// passes without a fold.
	}
}

// instanceRestartAction starts an instance unless it was stopped on purpose, and
// reports the outcome. stops says the restart is for an unhealthy run, so a
// running instance is force-stopped first; without it the restart is for a
// stop, and an instance found running - started since by someone else - is
// left as it is.
func instanceRestartAction(ctx context.Context, conn *iclient.Connection, project string, name string, stops bool) instanceResult {
	res := instanceResult{kind: instanceResultRestarted, name: name, project: project}

	inst, err := getInstance(ctx, conn, project, name)
	switch {
	case err != nil:
		res.err = err
		return res
	case util.IsTrue(inst.Config[shared.HealthStoppedKey]):
		res.err = ErrIntentionallyStopped
		return res
	default:
		state, _, err := conn.GetInstanceState(ctx, project, name)
		if err != nil {
			res.err = err
			return res
		}

		if !stops && runningStatus(state.StatusCode) {
			res.alreadyRunning = true
			return res
		}

		if state.StatusCode != incusApi.Stopped {
			stopReq := incusApi.InstanceStatePut{
				Action:  "stop",
				Timeout: -1,
				Force:   true,
			}

			err := setInstanceState(ctx, conn, project, name, stopReq)
			if err != nil {
				res.err = err
				return res
			}
		}
	}

	res.err = setInstanceState(ctx, conn, project, name, incusApi.InstanceStatePut{
		Action:  "start",
		Timeout: -1,
	})

	return res
}

// setInstanceState runs a state change to its end. The wait is inside the
// retry because Incus reports the instance's operation lock on the operation,
// not on the request that started it.
func setInstanceState(ctx context.Context, conn *iclient.Connection, project string, name string, state incusApi.InstanceStatePut) error {
	return retryBusy(ctx, conn, project, name, func() error {
		op, err := conn.UpdateInstanceState(ctx, project, name, state, "")
		if err != nil {
			return err
		}

		_, err = iclient.WaitOperation(ctx, op)

		return err
	})
}

func instanceExec(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, name string, run command) (int, string, string, error) {
	var stdout, stderr bytes.Buffer

	post := incusApi.InstanceExecPost{Command: run.cmd, Cwd: run.cwd, User: run.uid, Group: run.gid}

	updates, err := conn.ExecInstance(ctx, project, name, post, &iclient.InstanceExecArgs{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return -1, "", "", err
	}

	// The channel closes once the output has drained too, so by here the
	// buffers hold everything the command wrote.
	op, err := iclient.WaitOperation(ctx, updates)
	if err != nil {
		cancelExec(ctx, logger, conn, project, name, op)

		return -1, stdout.String(), stderr.String(), err
	}

	exitCode, ok := op.Metadata["return"].(float64)
	if !ok {
		return -1, "", "", nil
	}

	return int(exitCode), stdout.String(), stderr.String(), nil
}

// cancelExec asks the server to reap an exec the checker gave up on, so the
// command does not keep running in the instance after its probe timed out.
func cancelExec(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, name string, op incusApi.Operation) {
	if op.ID == "" || ctx.Err() == nil {
		return
	}

	// ctx is already done, so the cancel needs a budget of its own.
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), apiTimeout)
	defer cancel()

	err := conn.CancelOperation(cancelCtx, project, op)
	if err != nil {
		logger.Debug("Canceling exec operation", "instance", name, "error", err)
	}
}

// runningStatus says whether an instance state is a running one. Ready is
// Running with the guest's own word that it is up, given through /dev/incus;
// the enricher reads it the same way (ievent/enricher/state.go). Frozen and
// the transitions are not running: there is nothing to exec into.
func runningStatus(code incusApi.StatusCode) bool {
	return code == incusApi.Running || code == incusApi.Ready
}

func instanceCheckAction(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, name string, cfg *instanceConfig) instanceResult {
	res := instanceResult{kind: instanceResultChecked, name: name, project: project}

	inst, _, err := conn.GetInstanceState(ctx, project, name)
	if err != nil {
		logger.Debug("Fetching instance status error", "instance", name, "error", err)

		res.err = err
		return res
	}

	if !runningStatus(inst.StatusCode) {
		logger.Log(ctx, shared.LevelTrace, "Instance is not running", "instance", name, "status", inst.Status)

		res.err = ErrNotRunning
		return res
	}

	if len(cfg.test) == 0 {
		res.err = errors.New("trying to run a check on a instance without a test")
		return res
	}

	run := command{cwd: cfg.cwd, uid: cfg.uid, gid: cfg.gid}
	switch cfg.test[0] {
	case "CMD":
		run.cmd = cfg.test[1:]
	case "CMD-SHELL":
		run.cmd = []string{"/bin/sh", "-c", cfg.test[1]}
	case "NONE":
		return res
	default:
		// Assume it's a direct command
		run.cmd = cfg.test
	}

	exitCode, stdout, stderr, err := instanceExec(ctx, logger, conn, project, name, run)
	if err != nil {
		logger.Debug("exec error", "error", err, "stdout", stdout, "stderr", stderr)
		res.err = err
		return res
	}

	if exitCode != 0 {
		res.err = fmt.Errorf("cmd failed, exit code: %d", exitCode)
	}

	return res
}

// instanceStatusAction writes user.healthcheck.status, the only key the daemon
// owns on a watched instance.
func instanceStatusAction(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, name, status string) instanceResult {
	res := instanceResult{kind: instanceResultStatus, name: name, project: project, status: status}
	res.err = patchInstanceConfig(ctx, logger, conn, project, name, map[string]string{shared.HealthStatusKey: status})

	return res
}

// reportStatus writes status when it differs from what the instance last had,
// so a write only happens on a transition.
func reportStatus(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, results chan<- instanceResult, inst *instance, status string) {
	if status == inst.status {
		return
	}

	forceStatus(ctx, logger, conn, project, results, inst, status)
}

// forceStatus writes status even when it is the one held, for when what is
// held cannot be trusted to be on the instance.
func forceStatus(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, results chan<- instanceResult, inst *instance, status string) {
	// Recorded before the write lands, so two writes in flight cannot invert.
	inst.status = status

	if inst.writing {
		inst.queued = status
		inst.queuedRun = inst.run

		return
	}

	writeStatus(ctx, logger, conn, project, results, inst, status, inst.run)
}

// reportStarting marks a freshly started instance starting, as docker does: it
// has no verdict until its first check, and whatever was on it before - the
// last run's healthy, or stopped - is not one about this run. An instance with
// nothing to run has nothing to wait for.
func reportStarting(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, results chan<- instanceResult, inst *instance) {
	if len(inst.config.test) == 0 || inst.config.test[0] == "NONE" {
		return
	}

	reportStatus(ctx, logger, conn, project, results, inst, shared.HealthStatusStarting)
}

// writeStatus starts one status write. Its result is what clears writing and
// sends whatever was queued behind it. run is the run the value was asked for
// in, which for a queued one is not always the run it goes out in.
func writeStatus(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, project string, results chan<- instanceResult, inst *instance, status string, run uint64) {
	inst.writing = true

	write := &statusWrite{status: status}
	inst.write = write

	go func(name string) {
		res := instanceStatusAction(ctx, logger, conn, project, name, status)
		res.run = run
		res.write = write

		select {
		case <-ctx.Done():
		case results <- res:
		}
	}(inst.name)
}

// checkFailed applies one failed check and returns the status to report. The
// watchdog goes through here too: as in docker, a timed-out probe is a failure.
func checkFailed(inst *instance, now time.Time) string {
	// Here and not only on the result path: a run whose probes all time out
	// reaches its verdicts through the watchdog alone, and would otherwise
	// stay inside its start period, starting for good, never restarted.
	if inst.inRestart && !now.Before(inst.restartDone) {
		inst.inRestart = false
	}

	if inst.inRestart {
		// As in docker, failures inside the start period are not counted.
		return shared.HealthStatusStarting
	}

	inst.failures++

	if inst.failures < inst.config.retries {
		return inst.status
	}

	// Retries exhausted: hand it to the restart policy and count afresh.
	if slices.Contains(shared.RestartPolicies, inst.config.restart) {
		inst.failures = 0
		inst.action = instanceActionRestart
		inst.restartStops = true
		inst.due = now.Add(inst.restartDelay)
		inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)
	}

	return shared.HealthStatusUnhealthy
}

func runInstanceActions(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, pool pools, instances map[string]*instance, resultChan chan<- instanceResult, metrics bool) time.Time {
	var earliest time.Time
	now := time.Now()

	keep := func(due time.Time) {
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
	}

	// defer reschedules an action no worker was free for.
	deferAction := func(inst *instance) {
		logger.Debug("Deferring an action, every worker is busy", "project", inst.project, "instance", inst.name, "action", inst.action)

		inst.due = now.Add(poolRetryDelay + rand.N(poolRetryDelay)) // nolint:gosec
		keep(inst.due)
	}

	for _, inst := range instances {
		if inst.state != instanceIdle {
			// Not overrun yet: the loop has to wake for the watchdog by then,
			// or a check that never returns holds the instance until some
			// unrelated event comes by. A parked instance has no deadline.
			if !inst.actionDeadline.IsZero() && !now.After(inst.actionDeadline) {
				keep(inst.actionDeadline)
			}

			if !inst.actionDeadline.IsZero() && now.After(inst.actionDeadline) {
				logger.Warn("Action deadline exceeded", "project", inst.project, "instance", inst.name, "state", inst.state)
				// Clearing the context marks what the abandoned action reports as stale.
				inst.actionCancel()

				checking := inst.state == instanceChecking

				inst.state = instanceIdle
				inst.actionContext = nil
				inst.actionCancel = nil
				inst.actionDeadline = time.Time{}

				if checking && inst.actionRun != inst.run {
					// A probe of the run before: its timeout is not a failure
					// of this one, which has had no check yet.
					inst.due = now
				} else if checking {
					// As in docker, a probe that exceeds its timeout is a
					// failed probe.
					reportStatus(ctx, logger, conn, inst.project, resultChan, inst, checkFailed(inst, now))
					if metrics {
						checksTotal.WithLabelValues("failed").Inc()
					}

					// checkFailed sets its own deadline when it escalates.
					if inst.action != instanceActionRestart {
						inst.due = now.Add(inst.config.interval)
					}
				} else {
					// A restart that timed out is a restart that failed.
					inst.due = now.Add(inst.restartDelay)
					inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)
					if metrics {
						restartsTotal.WithLabelValues("failed").Inc()
					}
				}

				// Whichever branch set it, an escalation included: the
				// instance is idle again and waits on this.
				keep(inst.due)
			}
			continue
		}

		if now.Before(inst.due) {
			keep(inst.due)
			continue
		}

		switch inst.action {
		case instanceActionRestart:
			actionCtx, cancel := context.WithCancel(ctx)
			name, stops := inst.name, inst.restartStops

			logger.Info("Restarting", "instance", name)
			err := pool.restart.Submit(func() {
				res := instanceRestartAction(actionCtx, conn, inst.project, name, stops)
				res.ctx = actionCtx

				select {
				case <-actionCtx.Done():
				case resultChan <- res:
				}
			})
			if err != nil {
				cancel()
				deferAction(inst)
				if metrics {
					poolRefusals.WithLabelValues("restart").Inc()
				}
				continue
			}

			inst.state = instanceRestarting
			inst.actionDeadline = now.Add(restartTimeout)
			inst.actionContext, inst.actionCancel = actionCtx, cancel
			keep(inst.actionDeadline)
		case instanceActionCheck:
			// No need to check an instance without a test, or one that is not running.
			if len(inst.config.test) == 0 || !inst.config.running {
				continue
			}

			actionCtx, cancel := context.WithCancel(ctx)
			name, cfg := inst.name, inst.config

			err := pool.check.Submit(func() {
				res := instanceCheckAction(actionCtx, logger, conn, inst.project, name, cfg)
				res.ctx = actionCtx

				select {
				case <-actionCtx.Done():
				case resultChan <- res:
				}
			})
			if err != nil {
				cancel()
				deferAction(inst)
				if metrics {
					poolRefusals.WithLabelValues("check").Inc()
				}

				continue
			}

			// Committed only once the action is running: a refused one keeps
			// its idle state, and the watchdog never sees a deadline it never had.
			inst.state = instanceChecking
			inst.actionDeadline = now.Add(inst.config.timeout)
			inst.actionContext, inst.actionCancel = actionCtx, cancel
			inst.actionRun = inst.run
			keep(inst.actionDeadline)
		}
	}

	return earliest
}

// handleInstanceResult applies one action's outcome. Like handleInstanceEvent it
// must not block: the status write it may start runs on its own goroutine.
func handleInstanceResult(ctx context.Context, logger *slog.Logger, conn *iclient.Connection, instances map[string]*instance, results chan<- instanceResult, res instanceResult, metrics bool) {
	switch res.kind {
	case instanceResultChecked:
		inst, ok := instances[res.Key()]
		if !ok {
			logger.Error("Got check result for an unknown instance", "instance", res.name, "result", res.err)
			return
		}

		if res.ctx != inst.actionContext {
			logger.Debug("Dropping a stale check result", "instance", res.name)
			return
		}

		inst.state = instanceIdle

		// Releasing the context is what unhooks it from evCtx.
		inst.actionCancel()
		inst.actionCancel = nil
		inst.actionContext = nil
		inst.actionDeadline = time.Time{}

		now := time.Now()

		// Sent before a start that has landed since: the verdict, pass or
		// fail, is the last run's. The new run gets its own check at once.
		if inst.actionRun != inst.run {
			logger.Debug("Dropping the verdict of a check on the previous run", "instance", res.name)
			inst.due = now

			return
		}

		if inst.inRestart && !now.Before(inst.restartDone) {
			inst.inRestart = false
		}

		if res.config != nil && !inst.config.equals(res.config) {
			inst.config = res.config
		}

		// The instance stopped, which is a lifecycle fact and not a health
		// verdict, so it earns no failure. A stop event may never arrive -
		// one already stopped at discovery never had one - so the restart is
		// scheduled here too, and the event path leaves a queued one alone.
		if errors.Is(res.err, ErrNotRunning) {
			inst.config.running = false
			reportStatus(ctx, logger, conn, res.project, results, inst, shared.HealthStatusStopped)

			if slices.Contains(shared.RestartPolicies, inst.config.restart) {
				inst.action = instanceActionRestart
				inst.restartStops = false
				inst.due = time.Now().Add(inst.restartDelay)
				inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)
			}

			return
		}

		var want string

		// The first pass of a run started under the daemon's eyes is
		// written whatever status says.
		force := false

		if res.err == nil {
			// Spent when the write lands, not here: a write that fails
			// leaves status open to a stale read again.
			force = inst.unproven

			inst.failures = 0
			want = shared.HealthStatusHealthy

			// Staying up is what earns a fresh restart budget.
			inst.restartDelay = baseRestartDelay(inst.config)

			// As in docker, the first success ends the start period early.
			inst.inRestart = false
			if metrics {
				checksTotal.WithLabelValues("passed").Inc()
			}
		} else {
			logger.Debug("Check failed", "instance", inst.name, "inStart", inst.inRestart,
				"failures", inst.failures, "retries", inst.config.retries, "error", res.err)

			want = checkFailed(inst, now)
			if metrics {
				checksTotal.WithLabelValues("failed").Inc()
			}
		}

		if force {
			forceStatus(ctx, logger, conn, res.project, results, inst, want)
		} else {
			reportStatus(ctx, logger, conn, res.project, results, inst, want)
		}

		// A stop in flight or an escalation already set the deadline it wants.
		if inst.action == instanceActionRestart {
			return
		}

		interval := inst.config.interval
		if inst.inRestart {
			interval = inst.config.startInterval
		}

		inst.due = now.Add(interval + rand.N(interval/4)) // nolint:gosec
	case instanceResultRestarted:
		inst, ok := instances[res.Key()]
		if !ok {
			logger.Error("Got start result for an unknown instance", "instance", res.name, "result", res.err)
			return
		}

		if res.ctx != inst.actionContext {
			logger.Debug("Dropping a stale restart result", "instance", res.name)
			return
		}

		inst.actionCancel()
		inst.actionCancel = nil
		inst.actionContext = nil
		inst.actionDeadline = time.Time{}

		if res.err != nil {
			if errors.Is(res.err, ErrIntentionallyStopped) {
				// The user stopped it; only a start event brings it back.
				inst.state = instanceParked
				return
			}

			logger.Error("Restart failed", "instance", res.name, "error", res.err)
			if metrics {
				restartsTotal.WithLabelValues("failed").Inc()
			}

			inst.state = instanceIdle
			inst.action = instanceActionRestart
			inst.due = time.Now().Add(inst.restartDelay)
			inst.restartDelay = min(inst.restartDelay*2, maxRestartDelay)

			return
		}

		if res.alreadyRunning {
			// Started by someone else since the stop: nothing was touched,
			// and the run it is in has not been checked yet.
			logger.Info("Found running, not restarted", "instance", inst.name)
		} else {
			logger.Info("Restarted", "instance", inst.name)
			if metrics {
				restartsTotal.WithLabelValues("success").Inc()
			}
		}
		instanceStarted(inst, time.Now())
		reportStarting(ctx, logger, conn, res.project, results, inst)
	case instanceResultStatus:
		inst, ok := instances[res.Key()]
		if !ok {
			return
		}

		// Sent for an instance deleted since, and held now by the one created
		// under its name: releasing this one's writer would let two of its
		// writes race. Its own result is still to come.
		if res.write != inst.write {
			logger.Debug("Dropping the result of a status write for a deleted instance", "instance", res.name, "status", res.status)

			return
		}

		inst.writing = false
		inst.write = nil

		queued, queuedRun := inst.queued, inst.queuedRun
		inst.queued, inst.queuedRun = "", 0

		if res.err != nil {
			logger.Warn("Writing the health status failed",
				"instance", res.name, "status", res.status, "error", res.err)

			// Nothing landed, so forget what reportStatus recorded.
			if queued == "" && inst.status == res.status {
				inst.status = ""
			}
		} else {
			// Only transitions get here, so this stays quiet on a healthy fleet.
			logger.Info("Health status", "project", res.project, "instance", res.name, "status", res.status)

			// The run's own healthy is on the instance: from here a read
			// saying healthy can be believed. One sent in an earlier run
			// says nothing about this one.
			if res.status == shared.HealthStatusHealthy && res.run == inst.run {
				inst.unproven = false
			}
		}

		// What was asked for meanwhile, unless it is what just landed. A
		// write asked for in another run does not stand in for the queued
		// one: the same value, but only a run's own healthy proves it, and a
		// run left unproven forces every pass until one does. The queued
		// value keeps the run it was asked for in, so a healthy queued
		// before a start cannot go out as proof of the run after it.
		if queued != "" && (queued != res.status || res.err != nil || queuedRun != res.run) {
			writeStatus(ctx, logger, conn, res.project, results, inst, queued, queuedRun)
		}
	}
}
