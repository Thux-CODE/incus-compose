package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/compose-spec/compose-go/v2/types"
	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/lxc/incus/v7/shared/util"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v3"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/project"
	"github.com/lxc/incus-compose/shared"
)

const (
	// rolloutHealthMargin is added to the time a healthcheck needs for a
	// verdict, for ic-healthd to notice the start and write it.
	rolloutHealthMargin = 30 * time.Second

	rolloutPoll = 2 * time.Second
)

func newRolloutCommand() *cli.Command {
	return &cli.Command{
		Name:      "rollout",
		Usage:     "Replace a service's replicas one at a time, each new one healthy before an old one goes",
		Category:  "extensions",
		ArgsUsage: "SERVICE...",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "pull",
				Usage:   `Pull image before creating the new replicas ("always"|"missing"|"never")`,
				Value:   "missing",
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_PULL"),
			},
			&cli.DurationFlag{
				Name:    "timeout",
				Usage:   "Timeout for starting a new replica, before its healthcheck",
				Value:   2 * time.Minute,
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_TIMEOUT"),
			},
			&cli.DurationFlag{
				Name:    "health-timeout",
				Usage:   "How long a new replica has to become healthy (0 = start_period + retries * (interval + timeout) + 30s)",
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_HEALTH_TIMEOUT"),
			},
			&cli.DurationFlag{
				Name:    "dns-ttl",
				Usage:   "TTL ic-dns serves its records with; a drained replica keeps running this long, plus --drain-delay",
				Value:   5 * time.Second,
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_DNS_TTL"),
			},
			&cli.DurationFlag{
				Name:    "drain-delay",
				Usage:   "Extra time a drained replica keeps running, for connections in flight",
				Value:   5 * time.Second,
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_DRAIN_DELAY"),
			},
			&cli.DurationFlag{
				Name:    "stop-timeout",
				Usage:   "Timeout for stopping an old replica when the service sets no stop_grace_period",
				Value:   10 * time.Second,
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_STOP_TIMEOUT"),
			},
			&cli.BoolFlag{
				Name:    "keep-failed",
				Usage:   "Leave a new replica that did not become healthy stopped instead of deleting it",
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_KEEP_FAILED"),
			},
			&cli.BoolFlag{
				Name:    "external-healthd",
				Usage:   "Use healthd but do not try to look it up",
				Sources: cli.EnvVars("INCUS_COMPOSE_EXTERNAL_HEALTHD"),
			},
			&cli.BoolFlag{
				Name:    "no-dns",
				Usage:   "Don't point the new replicas at the project's ic-dns",
				Sources: cli.EnvVars("INCUS_COMPOSE_NO_DNS", "INCUS_COMPOSE_DISABLE_DNS"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			// The first signal winds the rollout down; a second one kills it.
			context.AfterFunc(ctx, stop)

			p, c, err := loadProject(ctx, cmd)
			if err != nil {
				return err
			}

			err = c.Open()
			if err != nil {
				c.LogError("Opening the project client", "error", err)
				return errLogged.Wrap(err)
			}
			defer c.WarnError(c.Done, "Failure during Client.Done()")

			services := cmd.Args().Slice()
			if len(services) == 0 {
				c.LogError("Name the services to roll out")
				return errLogged.WithText("no service")
			}

			for _, s := range services {
				_, ok := p.Services[s]
				if !ok {
					c.LogError("Service not found", "service", s)
					return errLogged.Wrap(client.ErrNotFound.WithKindName(client.KindInstance, s))
				}
			}

			if cmd.Bool("external-healthd") {
				p.ClientConfig.Healthd.External = true
			}

			startOpts := []client.Option{client.OptionTimeout(cmd.Duration("timeout"))}
			if p.ClientConfig.Healthd.External {
				startOpts = append(startOpts, client.OptionExternalHealthd())
			} else {
				running, err := c.HealthdRunning()
				if err != nil || !running {
					c.LogError("A rollout waits for ic-healthd to call each new replica healthy, and none watches this project; run `up` first", "error", err)
					return errLogged.WithText("no ic-healthd")
				}
			}

			if cmd.Bool("no-dns") {
				p.ClientConfig.DNS.Disabled = true
			}

			if !p.ClientConfig.DNS.Disabled {
				dnsIP, err := resolveDNSIP(ctx, c, cmd.Duration("timeout"))
				if err != nil {
					c.LogError("Getting dns daemon IP", "error", err)
					return errLogged.Wrap(err)
				}

				if p.InstanceMarks == nil {
					p.InstanceMarks = map[string]string{}
				}

				p.InstanceMarks["oci.dns.nameservers"] = dnsIP
				p.InstanceMarks["oci.dns.search"] = strings.TrimSuffix(dnsZone(p, c), ".")
			}

			c.IgnoreError(client.ActionStart, client.ErrRunning)
			c.IgnoreError(client.ActionStop, client.ErrNotRunning)

			err = c.RegisterDNSWatcher()
			if err != nil {
				c.LogError("Registering the DNS watcher", "project", p.Name, "error", err)
				return errLogged.Wrap(err)
			}

			order, err := p.ServiceOrder(false)
			if err != nil {
				c.LogError("Getting the service dependency order", "error", err)
				return errLogged.Wrap(err)
			}

			pullMode := client.PullMissing
			switch cmd.String("pull") {
			case "always":
				pullMode = client.PullAlways
			case "never":
				pullMode = client.PullNever
			}

			args := rolloutArgs{
				Pull:          pullMode,
				HealthTimeout: cmd.Duration("health-timeout"),
				StopTimeout:   cmd.Duration("stop-timeout"),
				Grace:         cmd.Duration("dns-ttl") + cmd.Duration("drain-delay"),
				KeepFailed:    cmd.Bool("keep-failed"),
				Order:         order,
				StartOptions:  startOpts,
				Workers:       cmd.Root().Int("workers"),
				Debug:         cmd.Root().Bool("debug"),
				NoColor:       noColor(ctx),
				Writer:        cmd.Root().Writer,
			}

			// Every service is checked before any is touched, so a refusal
			// never lands halfway through.
			type job struct {
				runner *rolloutRunner
				plan   rolloutPlan
			}

			var (
				jobs []job
				errs error
			)

			for _, s := range services {
				runner, plan, err := rolloutPrepare(ctx, p, c, s, args)
				if err != nil {
					c.LogError("Cannot roll out", "service", s, "error", err)
					errs = errors.Join(errs, err)

					continue
				}

				jobs = append(jobs, job{runner: runner, plan: plan})
			}

			if errs != nil {
				return errLogged.Wrap(errs)
			}

			for _, j := range jobs {
				err := rollout(ctx, j.runner, j.plan)
				if err != nil {
					c.LogError("Rolling out", "service", j.plan.Service, "error", err)
					return errLogged.Wrap(err)
				}
			}

			return nil
		},
	}
}

// rolloutArgs holds what rolloutPrepare reads off the command.
type rolloutArgs struct {
	Pull          client.PullMode
	HealthTimeout time.Duration
	StopTimeout   time.Duration
	Grace         time.Duration
	KeepFailed    bool
	Order         []string
	StartOptions  []client.Option
	Workers       int
	Debug         bool
	NoColor       bool
	Writer        io.Writer
}

// rolloutPrepare reads what a service has, pulls its image and checks that the
// new replicas can run next to the old ones.
func rolloutPrepare(ctx context.Context, p *project.Project, c *client.Client, svcName string, args rolloutArgs) (*rolloutRunner, rolloutPlan, error) {
	service := p.Services[svcName]

	n := 1
	if service.Deploy != nil && service.Deploy.Replicas != nil {
		n = *service.Deploy.Replicas
	}

	if n < 1 {
		return nil, rolloutPlan{}, errors.New("deploy.replicas is 0, there is nothing to roll out")
	}

	name := func(index int) string {
		return client.SanitizeIncusName(fmt.Sprintf("%s-%d", svcName, index), -1)
	}

	// The old replicas are read through a client of their own, with no compose
	// config behind them, so nothing here adds keys to them.
	oc := c.Clone()

	var (
		present []replica
		first   *incusApi.Instance
	)

	instances := map[int]*client.Instance{}

	for i := 1; i <= n+2; i++ {
		exists, err := c.InstanceExists(name(i))
		if err != nil {
			return nil, rolloutPlan{}, err
		}

		if !exists {
			continue
		}

		if i == n+2 {
			return nil, rolloutPlan{}, fmt.Errorf("%s exists, more than %d replicas and a spare: run `up` to scale it back first", name(i), n)
		}

		res, err := oc.Resource(client.KindInstance, name(i), &client.InstanceConfig{})
		if err != nil {
			return nil, rolloutPlan{}, err
		}

		inst, ok := res.(*client.Instance)
		if !ok {
			return nil, rolloutPlan{}, client.ErrUnknown.WithResource(res)
		}

		err = inst.Ensure(ctx)
		if err != nil {
			return nil, rolloutPlan{}, err
		}

		info := inst.State().IncusInstance
		if info.Config[project.ServiceLabelKey] != svcName {
			return nil, rolloutPlan{}, fmt.Errorf("%s is not a replica of %s", name(i), svcName)
		}

		present = append(present, replica{
			index:   i,
			running: inst.Running(),
			drained: util.IsTrue(info.Config[shared.DNSDrainKey]),
		})
		instances[i] = inst

		if first == nil {
			first = info
		}
	}

	if first == nil {
		return nil, rolloutPlan{}, fmt.Errorf("%s has no replica to replace: create it with `up`", svcName)
	}

	pc := c.Clone()

	err := pull(ctx, p, pc, pullArgs{
		Services:        []string{svcName},
		IgnoreBuildable: true,
		Pull:            args.Pull,
		Workers:         args.Workers,
		Debug:           args.Debug,
		Writer:          args.Writer,
	})
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	// At n+1, so a cluster without shared storage refuses the spare here rather
	// than halfway through.
	resources, err := p.Resources(pc, project.ResourcesScale(map[string]int{svcName: n + 1}))
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	// Built images are not pulled; this builds one that is missing, as `up` does.
	images := client.NewStack(pc, client.StackWorkers(args.Workers), client.StackFailFast())
	images.AddOrdered(args.Order, map[string][]client.Resource{svcName: resources[svcName]})

	isImage := func(r client.Resource) bool { return r.Kind() == client.KindImage }

	err = images.ForActionF(client.ActionEnsure, isImage).Run(ctx, client.ActionEnsure, client.OptionCreate())
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	var health map[string]string
	for _, r := range resources[svcName] {
		inst, ok := r.(*client.Instance)
		if ok {
			health, err = inst.HealthConfig()
			if err != nil {
				return nil, rolloutPlan{}, err
			}

			break
		}
	}

	pools := map[string]string{}
	clustered := c.Global().IsClustered()

	if clustered {
		conn, err := c.GlobalConnection()
		if err != nil {
			return nil, rolloutPlan{}, err
		}

		for _, dev := range first.ExpandedDevices {
			if dev["type"] != "disk" || dev["path"] == "/" || dev["pool"] == "" {
				continue
			}

			pool, _, err := conn.GetStoragePool(ctx, dev["pool"])
			if err != nil {
				return nil, rolloutPlan{}, fmt.Errorf("getting storage pool %q: %w", dev["pool"], err)
			}

			pools[pool.Name] = pool.Driver
		}
	}

	blockers := rolloutBlockers(service, health, first, pools, clustered)
	if len(blockers) > 0 {
		return nil, rolloutPlan{}, fmt.Errorf("%s; `restart` it instead", strings.Join(blockers, "; "))
	}

	healthTimeout := args.HealthTimeout
	if healthTimeout == 0 {
		healthTimeout = rolloutHealthTimeout(health)
	}

	stopTimeout := args.StopTimeout
	if service.StopGracePeriod != nil {
		stopTimeout = time.Duration(*service.StopGracePeriod)
	}

	runner := &rolloutRunner{
		p:           p,
		c:           c,
		service:     svcName,
		replicas:    n,
		name:        name,
		instances:   instances,
		order:       args.Order,
		startOpts:   args.StartOptions,
		stopTimeout: stopTimeout,
		workers:     args.Workers,
		debug:       args.Debug,
		noColor:     args.NoColor,
		writer:      args.Writer,
	}

	plan := rolloutPlan{
		Service:       svcName,
		Replicas:      n,
		Present:       present,
		Name:          name,
		KeepFailed:    args.KeepFailed,
		HealthTimeout: healthTimeout,
		Grace:         args.Grace,
		Poll:          rolloutPoll,
		Now:           time.Now,
		Out:           args.Writer,
	}

	return runner, plan, nil
}

// rolloutBlockers says why the new replicas of a service cannot run next to
// the old ones, nothing when they can. old is a replica as Incus has it, so
// what profiles, x-incus and the image added is seen too.
func rolloutBlockers(service types.ServiceConfig, health map[string]string, old *incusApi.Instance, pools map[string]string, clustered bool) []string {
	var out []string

	add := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		if !slices.Contains(out, msg) {
			out = append(out, msg)
		}
	}

	var ext struct {
		Rollout *bool `mapstructure:"rollout"`
	}

	ok, err := service.Extensions.Get("x-incus-compose", &ext)
	if err == nil && ok && ext.Rollout != nil && !*ext.Rollout {
		add("x-incus-compose.rollout: false marks it as one that must not run twice")
	}

	if !rolloutWatched(health) {
		add("it has no healthcheck, so no new replica can be waited for and ic-dns serves it before it is ready")
	}

	if service.ContainerName != "" {
		add("container_name %q is one name, which the new and the old replica cannot share", service.ContainerName)
	}

	if service.Deploy != nil && service.Deploy.Mode == "global" {
		add("deploy.mode: global keeps one replica per cluster member")
	}

	if len(service.Ports) > 0 {
		add("its published ports cannot be bound by the new and the old replica at once")
	}

	fixed := service.MacAddress != ""
	aliases := old.Config["user.label.dns.aliases"] != "" || service.Labels["dns.aliases"] != ""

	for _, net := range service.Networks {
		if net == nil {
			continue
		}

		fixed = fixed || net.Ipv4Address != "" || net.Ipv6Address != "" || net.MacAddress != ""
		aliases = aliases || len(net.Aliases) > 0
	}

	for _, devName := range slices.Sorted(maps.Keys(old.ExpandedDevices)) {
		dev := old.ExpandedDevices[devName]

		switch dev["type"] {
		case "nic":
			fixed = fixed || dev["ipv4.address"] != "" || dev["ipv6.address"] != "" || dev["hwaddr"] != ""
		case "proxy":
			add("its proxy device %q binds a host address the new and the old replica cannot both hold", devName)
		case "disk":
			if dev["path"] == "/" || dev["pool"] == "" || !clustered || pools[dev["pool"]] == "cephfs" {
				continue
			}

			add("volume %q on pool %q (%s) is attached on one cluster member at a time, and the new replica may land on another", dev["source"], dev["pool"], pools[dev["pool"]])
		}
	}

	if fixed {
		add("a fixed IP or MAC address cannot be held by the new and the old replica at once")
	}

	if aliases {
		add("its DNS aliases are answered by replica 1 alone and would go unanswered while it is replaced")
	}

	return out
}

// rolloutWatched reports whether ic-healthd runs a check on an instance with
// this config: enabled, with a test that is not compose's NONE.
func rolloutWatched(health map[string]string) bool {
	if !util.IsTrue(health[shared.HealthEnabledKey]) {
		return false
	}

	raw := strings.TrimSpace(health[shared.HealthKeyPrefix+"test"])
	if raw == "" {
		return false
	}

	var test []string

	err := json.Unmarshal([]byte(raw), &test)
	if err != nil {
		return true
	}

	return len(test) > 0 && test[0] != "NONE"
}

// rolloutHealthTimeout is the longest ic-healthd can take to reach a verdict
// on a new replica with this config, with docker's defaults for what is unset.
func rolloutHealthTimeout(health map[string]string) time.Duration {
	duration := func(key string, def time.Duration) time.Duration {
		d, err := time.ParseDuration(health[shared.HealthKeyPrefix+key])
		if err != nil || d <= 0 {
			return def
		}

		return d
	}

	retries := 3

	var n int

	_, err := fmt.Sscan(health[shared.HealthKeyPrefix+"retries"], &n)
	if err == nil && n > 0 {
		retries = n
	}

	return duration("start_period", 0) +
		time.Duration(retries)*(duration("interval", 30*time.Second)+duration("timeout", 30*time.Second)) +
		rolloutHealthMargin
}

// rolloutRunner carries out a rollout's steps on the replicas of one service.
type rolloutRunner struct {
	p        *project.Project
	c        *client.Client
	service  string
	replicas int
	name     func(index int) string

	// instances is what drain, retire and discard act on: the old replicas as
	// read at the start, and the new ones as created.
	instances map[int]*client.Instance

	order       []string
	startOpts   []client.Option
	stopTimeout time.Duration
	workers     int
	debug       bool
	noColor     bool
	writer      io.Writer
}

func (r *rolloutRunner) create(ctx context.Context, index int) error {
	// A client of its own per replica, so the instance is built from the
	// compose file as it is now and not from one this run built earlier.
	cc := r.c.Clone()

	resources, err := r.p.Resources(cc, project.ResourcesScale(map[string]int{r.service: max(r.replicas, index)}))
	if err != nil {
		return err
	}

	var inst *client.Instance
	for _, res := range resources[r.service] {
		i, ok := res.(*client.Instance)
		if ok && i.IncusName() == r.name(index) {
			inst = i
		}
	}

	if inst == nil {
		return client.ErrNotFound.WithKindName(client.KindInstance, r.name(index))
	}

	if !r.debug {
		progress := newProgressRenderer(r.writer, r.noColor, isatty.IsTerminal(os.Stdout.Fd()))
		progress.Start(cc)
		defer progress.Stop(cc)
	}

	stack := client.NewStack(cc, client.StackWorkers(r.workers), client.StackFailFast())
	stack.AddOrdered(r.order, map[string][]client.Resource{r.service: resources[r.service]})

	only := func(res client.Resource) bool {
		return res.Kind() != client.KindInstance || res == client.Resource(inst)
	}

	// An interrupted create would go on in Incus without us, so it is let
	// finish and taken away after.
	err = stack.ForActionF(client.ActionEnsure, only).Run(context.WithoutCancel(ctx), client.ActionEnsure, append(slices.Clone(r.startOpts), client.OptionCreate())...)
	if err != nil {
		r.instances[index] = inst
		return err
	}

	if !inst.Created() {
		return fmt.Errorf("%s was created by someone else meanwhile", r.name(index))
	}

	r.instances[index] = inst

	// The healthcheck is waited for by the rollout, which also gives up on an
	// unhealthy verdict, and the dependencies are up already.
	return inst.Start(ctx, append(slices.Clone(r.startOpts), client.OptionNoHealthd())...)
}

func (r *rolloutRunner) health(ctx context.Context, index int) (string, bool, error) {
	conn, err := r.c.Connection()
	if err != nil {
		return "", false, err
	}

	inst, _, err := conn.GetInstance(ctx, r.c.IncusProject(), r.name(index), nil)
	if err != nil {
		return "", false, err
	}

	return inst.Config[shared.HealthStatusKey], inst.StatusCode == incusApi.Running, nil
}

func (r *rolloutRunner) drain(ctx context.Context, index int, drained bool) error {
	return r.instances[index].SetDrained(ctx, drained)
}

func (r *rolloutRunner) retire(ctx context.Context, index int) error {
	inst := r.instances[index]

	err := inst.Stop(ctx, client.OptionTimeout(r.stopTimeout))
	if err != nil && !errors.Is(err, client.ErrNotRunning) {
		return err
	}

	err = inst.Delete(ctx)
	if err != nil {
		return err
	}

	delete(r.instances, index)

	return nil
}

func (r *rolloutRunner) discard(ctx context.Context, index int, keep bool) error {
	inst := r.instances[index]
	if inst == nil {
		return nil
	}

	if !inst.IsEnsured() {
		exists, err := r.c.InstanceExists(r.name(index))
		if err != nil || !exists {
			return err
		}

		err = inst.Ensure(ctx)
		if err != nil {
			return err
		}
	}

	opts := []client.Option{client.OptionForce()}
	if keep {
		opts = []client.Option{client.OptionTimeout(r.stopTimeout)}
	}

	err := inst.Stop(ctx, opts...)
	if err != nil && !errors.Is(err, client.ErrNotRunning) {
		return err
	}

	if keep {
		return nil
	}

	err = inst.Delete(ctx)
	if err != nil {
		return err
	}

	delete(r.instances, index)

	return nil
}

func (r *rolloutRunner) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
