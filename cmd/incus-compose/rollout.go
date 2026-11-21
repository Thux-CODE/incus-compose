package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
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
	"github.com/lxc/incus-compose/iclient"
	"github.com/lxc/incus-compose/project"
	"github.com/lxc/incus-compose/shared"
)

// The marks of a rollout run on the instance config, which a run picked up
// again goes by: the run that made a replica, that drained it, and whose
// service-wide pre_start hooks it ran. The first and the last are taken off
// once the rollout is over; a drain's goes with the replica it is on.
const (
	rolloutIDKey       = "user.incus-compose.rollout.id"
	rolloutDrainKey    = "user.incus-compose.rollout.drain"
	rolloutPreStartKey = "user.incus-compose.rollout.pre-start"
)

// rolloutNewRunID returns the id of a new rollout run.
func rolloutNewRunID() (string, error) {
	b := make([]byte, 8)

	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("making a rollout id: %w", err)
	}

	return hex.EncodeToString(b), nil
}

const (
	// rolloutHealthMargin is added to the time a healthcheck needs for a
	// verdict, for ic-healthd to notice the start and write it.
	rolloutHealthMargin = 30 * time.Second

	rolloutPoll = 2 * time.Second

	// rolloutGraceStep is how often the replicas that serve in place of a
	// drained one are read during its grace.
	rolloutGraceStep = 5 * time.Second

	// rolloutStopSettle is how long a replica being stopped may take to read
	// Stopped, after its graceful stop, before the rollout gives up on it; it
	// is killed every rolloutKillEvery while it reads running.
	rolloutStopSettle = 30 * time.Second
	rolloutKillEvery  = 5 * time.Second

	// rolloutRestartWindow is how long a new replica kept stopped is watched
	// for a restart ic-healthd had under way, and rolloutTakeDownAttempts how
	// many times a replica is stopped before the rollout gives up on it.
	rolloutRestartWindow    = 10 * time.Second
	rolloutTakeDownAttempts = 3
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
				Name:    "dry-run",
				Usage:   "Check the services and print what a rollout would do, creating, building, writing and stopping nothing (a missing image is still pulled)",
				Sources: cli.EnvVars("INCUS_COMPOSE_ROLLOUT_DRY_RUN"),
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

			// A dry run writes nothing, the project's config included.
			var (
				projectOpts []client.EnsureProjectOption
				missingKeys []string
			)

			dryRun := cmd.Bool("dry-run")
			if dryRun {
				projectOpts = append(projectOpts, client.EnsureProjectReadOnly(&missingKeys))

				// Nor the image cache's project, which Connect creates.
				globalClient, err := clientFromContext(ctx)
				if err != nil {
					return err
				}

				globalClient.SetReadOnly()
			}

			p, c, err := loadProject(ctx, cmd, projectOpts...)
			if err != nil {
				return err
			}

			err = c.Open()
			if err != nil {
				c.LogError("Opening the project client", "error", err)
				return errLogged.Wrap(err)
			}
			defer c.WarnError(c.Done, "Failure during Client.Done()")

			services := slices.Compact(slices.Clone(cmd.Args().Slice()))
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

			// No DNSWatcher: it writes a service's dnsmasq record from the
			// instances one run has seen, which here is the new replica alone,
			// before it is healthy. A rollout leaves the names to ic-dns.
			c.IgnoreError(client.ActionStart, client.ErrRunning)
			c.IgnoreError(client.ActionStop, client.ErrNotRunning)

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

			// A refresh deletes the copies of the image before it pulls the
			// new one, which a dry run does not: it pulls what is missing, and
			// the plan reads the alias as it is.
			var pullNote string
			if dryRun && pullMode == client.PullAlways {
				pullMode = client.PullMissing
				pullNote = "with --pull always a rollout refreshes the image first: the plan goes by the image the project has"
			}

			args := rolloutArgs{
				Pull:          pullMode,
				HealthTimeout: cmd.Duration("health-timeout"),
				StopTimeout:   cmd.Duration("stop-timeout"),
				Grace:         cmd.Duration("dns-ttl") + cmd.Duration("drain-delay"),
				KeepFailed:    cmd.Bool("keep-failed"),
				DryRun:        dryRun,
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

			if dryRun {
				for _, j := range jobs {
					if pullNote != "" {
						j.plan.Notes = append(j.plan.Notes, pullNote)
					}

					rolloutDescribe(j.plan)
				}

				if len(missingKeys) > 0 {
					_, _ = fmt.Fprintf(args.Writer, "project %s: a rollout would first add the config keys %s, which --dry-run does not write\n", p.Name, strings.Join(missingKeys, ", "))
				}

				return nil
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
	DryRun        bool
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

	// scale: or deploy.replicas, as up counts them.
	n := service.GetScale()

	if n < 1 {
		return nil, rolloutPlan{}, errors.New("the service declares 0 replicas, there is nothing to roll out")
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

	// The replicas are the ones Incus has, whatever their indexes: an earlier
	// rollout leaves them on others than 1 to N.
	indexes, taken, err := p.ReplicaIndexes(c, svcName)
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	if len(indexes) > n+1 {
		return nil, rolloutPlan{}, fmt.Errorf("%d replicas exist, more than %d and one in flight: run `up` to scale it back first", len(indexes), n)
	}

	for _, i := range indexes {
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
			index:     i,
			id:        info.Config["volatile.uuid"],
			created:   info.CreatedAt,
			running:   rolloutRunning(info.StatusCode),
			drained:   shared.DNSDrained(rolloutExpanded(info)[shared.DNSDrainKey]),
			drainedBy: info.Config[rolloutDrainKey],
			runID:     info.Config[rolloutIDKey],
			preStart:  info.Config[rolloutPreStartKey],
			healthy:   info.Config[shared.HealthStatusKey] == shared.HealthStatusHealthy,
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

	// The sidecars are not ours to touch, nor are their images in the global
	// project. A dry run pulls nothing: a pull may delete a copy it replaces
	// and creates the global project and its locks, and the plan reads the
	// image as the project has it.
	if !args.DryRun {
		err = pull(ctx, p, pc, pullArgs{
			Services:        []string{svcName},
			IgnoreBuildable: true,
			NoTools:         true,
			Pull:            args.Pull,
			Workers:         args.Workers,
			Debug:           args.Debug,
			Writer:          args.Writer,
		})
		if err != nil {
			return nil, rolloutPlan{}, err
		}
	}

	// At n+1, so a cluster without shared storage refuses the replica that
	// runs next to the old ones here rather than halfway through.
	resources, err := p.Resources(pc, project.ResourcesScale(map[string]int{svcName: n + 1}))
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	// Built images are not pulled; this builds one that is missing, as `up` does,
	// or copies it from the cache. A dry run does neither, and says so.
	images := client.NewStack(pc, client.StackWorkers(args.Workers), client.StackFailFast())
	images.AddOrdered(args.Order, map[string][]client.Resource{svcName: resources[svcName]})

	isImage := func(r client.Resource) bool { return r.Kind() == client.KindImage }

	var (
		notes   []string
		unbuilt bool
	)

	if args.DryRun {
		err = images.ForActionF(client.ActionEnsure, isImage).Run(ctx, client.ActionEnsure)

		// Only an image that is not there; a read that failed is an error.
		if err != nil && !errors.Is(err, client.ErrCreate) && !errors.Is(err, client.ErrNotFound) {
			return nil, rolloutPlan{}, err
		}

		if err != nil {
			unbuilt = true

			notes = append(notes, fmt.Sprintf("its image is not in the project: a rollout would build it, copy it from the cache or pull it first, which --dry-run does not (%v); the healthcheck an image declares is not known until then", err))
		}
	} else {
		err = images.ForActionF(client.ActionEnsure, isImage).Run(ctx, client.ActionEnsure, client.OptionCreate())
		if err != nil {
			return nil, rolloutPlan{}, err
		}
	}

	// Which replicas are already what a create would make now, read once the
	// image is pulled: a rollout picked up again keeps them.
	hash, err := project.ServiceConfigHash(service)
	if err != nil {
		return nil, rolloutPlan{}, err
	}

	var alias *incusApi.ImageAliasesEntry

	for _, r := range resources[svcName] {
		img, ok := r.(*client.Image)
		if ok {
			alias = img.State().IncusAlias
			break
		}
	}

	for i := range present {
		present[i].current = rolloutCurrent(instances[present[i].index].State().IncusInstance.Config, hash, alias)
	}

	// The image every new replica is made from, as checked here.
	var target string
	if alias != nil {
		target = alias.Target
	}

	// The run an interrupted one marked its replicas with is picked up again;
	// else this is a new one.
	runID := rolloutAdoptedRun(present)
	if runID == "" {
		runID, err = rolloutNewRunID()
		if err != nil {
			return nil, rolloutPlan{}, err
		}
	}

	uuids := map[int]string{}
	for _, r := range present {
		uuids[r.index] = r.id
	}

	var (
		health   map[string]string
		declared map[string]map[string]string

		// drainDeclared is a new replica made drained, by a label or x-incus.
		drainDeclared bool
	)

	for _, r := range resources[svcName] {
		inst, ok := r.(*client.Instance)
		if !ok {
			continue
		}

		if unbuilt {
			// The compose file's own healthcheck, the image's being unknown.
			health = map[string]string{}

			for key, value := range inst.Config.Extensions {
				if strings.HasPrefix(key, shared.HealthKeyPrefix) {
					health[key] = value
				}
			}
		} else {
			health, err = inst.HealthConfig()
			if err != nil {
				return nil, rolloutPlan{}, err
			}
		}

		declared = declaredDevices(inst, c.Config().DefaultStoragePool)
		drainDeclared = shared.DNSDrained(inst.Config.Extensions[shared.DNSDrainKey])

		break
	}

	pools := map[string]string{}
	clustered := c.Global().IsClustered()

	if clustered {
		conn, err := c.GlobalConnection()
		if err != nil {
			return nil, rolloutPlan{}, err
		}

		for _, devices := range []map[string]map[string]string{first.ExpandedDevices, declared} {
			for _, dev := range devices {
				_, known := pools[dev["pool"]]
				if dev["type"] != "disk" || dev["path"] == "/" || dev["pool"] == "" || known {
					continue
				}

				pool, _, err := conn.GetStoragePool(ctx, dev["pool"])
				if err != nil {
					return nil, rolloutPlan{}, fmt.Errorf("getting storage pool %q: %w", dev["pool"], err)
				}

				pools[pool.Name] = pool.Driver
			}
		}
	}

	health = rolloutProfileHealth(health, first)

	blockers := rolloutBlockers(service, health, first.Config, []map[string]map[string]string{first.ExpandedDevices, declared}, pools, clustered)
	if drainDeclared && !slices.Contains(blockers, rolloutDrainedBlocker) {
		blockers = append(blockers, rolloutDrainedBlocker)
	}

	// The image a dry run did not get may declare the healthcheck, unless the
	// compose file disables it, which a create honours.
	if unbuilt && !rolloutHealthDisabled(service) && slices.Contains(blockers, rolloutUnwatchedBlocker) {
		blockers = slices.DeleteFunc(blockers, func(b string) bool { return b == rolloutUnwatchedBlocker })
		notes = append(notes, "the compose file declares no healthcheck, and a rollout refuses the service unless its image does")
	}

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
		name:        name,
		instances:   instances,
		uuids:       uuids,
		runID:       runID,
		target:      target,
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
		Taken:         taken,
		RunID:         runID,
		PreStart:      slices.ContainsFunc(service.PreStart, func(h types.PreStartHook) bool { return !h.PerReplica }),
		Name:          name,
		KeepFailed:    args.KeepFailed,
		HealthTimeout: healthTimeout,
		Grace:         args.Grace,
		GraceStep:     rolloutGraceStep,
		Poll:          rolloutPoll,
		Now:           time.Now,
		Out:           args.Writer,
		Notes:         notes,
	}

	return runner, plan, nil
}

// declaredDevices returns the devices the compose file gives a new replica, in
// the shape Incus reports them.
func declaredDevices(inst *client.Instance, defaultPool string) map[string]map[string]string {
	out := map[string]map[string]string{}

	for _, dev := range inst.Config.Devices {
		m := maps.Clone(dev.Config.Extensions)
		if m == nil {
			m = map[string]string{}
		}

		m["type"] = dev.Config.DeviceType

		if dev.Config.DeviceType == client.InstanceDeviceTypeDisk && dev.Config.Disk.StorageVolumeConfig != nil {
			m["pool"] = dev.Config.Disk.StorageVolumeConfig.Pool
			if m["pool"] == "" {
				m["pool"] = defaultPool
			}

			m["source"] = dev.Config.Disk.Source
			m["path"] = dev.Config.Disk.Path
		}

		out[dev.Name] = m
	}

	maps.Copy(out, inst.Config.ExtraDevices)

	return out
}

// rolloutProfileHealth adds to the healthcheck the compose file and the image
// give a new replica what a profile gives it: the keys the old replica has
// from its profiles alone, which ic-healthd runs as well. What the compose
// file or the image sets wins.
func rolloutProfileHealth(health map[string]string, old *incusApi.Instance) map[string]string {
	out := maps.Clone(health)
	if out == nil {
		out = map[string]string{}
	}

	for k, v := range old.ExpandedConfig {
		_, local := old.Config[k]
		_, declared := out[k]

		if strings.HasPrefix(k, shared.HealthKeyPrefix) && !local && !declared {
			out[k] = v
		}
	}

	return out
}

// rolloutHealthDisabled reports a compose file that turns the healthcheck off,
// the image's included: disable, or a test of NONE.
func rolloutHealthDisabled(service types.ServiceConfig) bool {
	h := service.HealthCheck

	return h != nil && (h.Disable || len(h.Test) > 0 && h.Test[0] == "NONE")
}

// rolloutUnwatchedBlocker refuses a service nothing checks.
const rolloutUnwatchedBlocker = "it has no healthcheck, so no new replica can be waited for and ic-dns serves it before it is ready"

// rolloutDrainedBlocker refuses a service whose new replicas would be made
// drained, which ic-dns keeps out of the service name: one would pass its
// check and an old one go, with nothing left serving under the name.
const rolloutDrainedBlocker = "its dns.drain label keeps every new replica out of the service name, so none would serve in place of an old one"

// rolloutBlockers says why the new replicas of a service cannot run next to
// the old ones, nothing when they can. oldConfig and the first device set are
// a replica as Incus has it, so what profiles and the image added is seen;
// the others are what the compose file declares now.
func rolloutBlockers(service types.ServiceConfig, health map[string]string, oldConfig map[string]string, deviceSets []map[string]map[string]string, pools map[string]string, clustered bool) []string {
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
	if err != nil {
		add("x-incus-compose.rollout must be true or false")
	} else if ok && ext.Rollout != nil && !*ext.Rollout {
		add("x-incus-compose.rollout: false marks it as one that must not run twice")
	}

	if !rolloutWatched(health) {
		add(rolloutUnwatchedBlocker)
	}

	if shared.DNSDrained(service.Labels[strings.TrimPrefix(shared.DNSDrainKey, "user.label.")]) {
		add(rolloutDrainedBlocker)
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
	aliases := oldConfig["user.label.dns.aliases"] != "" || service.Labels["dns.aliases"] != ""

	for _, net := range service.Networks {
		if net == nil {
			continue
		}

		fixed = fixed || net.Ipv4Address != "" || net.Ipv6Address != "" || net.MacAddress != ""
		aliases = aliases || len(net.Aliases) > 0
	}

	for _, devices := range deviceSets {
		for _, devName := range slices.Sorted(maps.Keys(devices)) {
			dev := devices[devName]

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
	}

	if fixed {
		add("a fixed IP or MAC address cannot be held by the new and the old replica at once")
	}

	if aliases {
		add("its DNS aliases are answered by the first replica alone and would go unanswered while it is replaced")
	}

	return out
}

// rolloutCurrent reports whether a replica with this config is what a create
// of the rollout would make now: its config hash is the service's, and it runs
// the image the service's alias points at, by the criteria `up` finds a
// replica stale with (Instance.ConfigChanged, pulledImageChanged). The image
// is compared whatever --pull says, since a create takes the alias as it is.
// One with no hash, made before there was one, is not current: `up` leaves it
// be so as not to recreate what an upgrade alone touched, but a rollout is
// asked to replace, and cannot tell it from one made from another config.
// Nor is one when the alias points at nothing known: the image is the one a
// create makes only when it is the alias's target.
func rolloutCurrent(config map[string]string, hash string, alias *incusApi.ImageAliasesEntry) bool {
	h, ok := config[client.ConfigHashKey]

	return ok && h == hash && alias != nil && alias.Target != "" && config["volatile.base_image"] == alias.Target
}

// shortFingerprint shortens an image fingerprint for a message, as Incus does.
func shortFingerprint(fp string) string {
	if len(fp) > 12 {
		return fp[:12]
	}

	if fp == "" {
		return "(unknown)"
	}

	return fp
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
	p       *project.Project
	c       *client.Client
	service string
	name    func(index int) string

	// instances is what drain, retire and discard act on: the old replicas as
	// read at the start, and the new ones as created.
	instances map[int]*client.Instance

	// uuids are the instances' volatile.uuid as first read: what is stopped,
	// deleted or taken away is checked to be still that instance.
	uuids map[int]string

	// runID marks what this run makes and drains (rolloutIDKey).
	runID string

	// target is the fingerprint the service's image pointed at when the plan
	// was made, or the one the first new replica was made from when that was
	// not known: every new replica of the run is made from it.
	target string

	order       []string
	startOpts   []client.Option
	stopTimeout time.Duration
	workers     int
	debug       bool
	noColor     bool
	writer      io.Writer
}

func (r *rolloutRunner) create(ctx context.Context, index int, first bool) (string, error) {
	err := r.createReplica(ctx, index, first)

	return r.uuids[index], err
}

func (r *rolloutRunner) createReplica(ctx context.Context, index int, first bool) error {
	// A client of its own per replica, so the instance is built from the
	// compose file as it is now and not from one this run built earlier.
	cc := r.c.Clone()

	resources, err := r.p.Resources(cc, project.ResourcesAddReplica(r.service, index, first))
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

	// The new replica is started with OptionNoHealthd, which also skips the
	// depends_on waits; a service_healthy dependency is read here instead, as
	// it stands, and not started or waited for: one that is not healthy now
	// stops the rollout before anything is created.
	err = r.dependenciesHealthy(ctx, inst)
	if err != nil {
		return err
	}

	// Said once what stands in its way is checked.
	_, _ = fmt.Fprintf(r.writer, "%s: creating\n", r.name(index))

	// What discard may delete is only what this run creates, so the name is
	// checked free first: an Ensure would adopt an instance found there.
	absent, err := instanceAbsent(ctx, r.c, r.name(index))
	if err != nil {
		return err
	}

	if !absent {
		return fmt.Errorf("%s exists already, made by something else than this rollout", r.name(index))
	}

	r.instances[index] = inst

	// The index may be one a replica retired earlier in the run held: its
	// uuid is not this instance's, which is known once it is created.
	delete(r.uuids, index)

	// Marked as this run's from its create on, and, the first one, as having
	// run the service-wide pre_start hooks once they succeed: a run picked up
	// again keeps it, and does not run them again.
	if inst.Config.Extensions == nil {
		inst.Config.Extensions = map[string]string{}
	}

	inst.Config.Extensions[rolloutIDKey] = r.runID

	if first {
		inst.Config.PreRunMark = map[string]string{rolloutPreStartKey: r.runID}
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
	// No adoption: one made by something else between the check and the
	// create fails the Ensure untouched.
	err = stack.ForActionF(client.ActionEnsure, only).Run(context.WithoutCancel(ctx), client.ActionEnsure, append(slices.Clone(r.startOpts), client.OptionCreate(), client.OptionNoAdopt())...)
	if err != nil {
		return err
	}

	if !inst.Created() {
		return fmt.Errorf("%s exists already, made by something else than this rollout", r.name(index))
	}

	var (
		made    string
		drained bool
	)

	if info := inst.State().IncusInstance; info != nil {
		made = info.Config["volatile.base_image"]
		drained = shared.DNSDrained(rolloutExpanded(info)[shared.DNSDrainKey])
		r.uuids[index] = info.Config["volatile.uuid"]
	}

	// A profile may drain it from its create on, which the compose file does
	// not tell: it would pass its check and serve nothing.
	if drained {
		return fmt.Errorf("%s was made drained, by a profile: %s keeps it out of the service name", r.name(index), shared.DNSDrainKey)
	}

	// Each create ensures the image again, on a client of its own: a pull in
	// between moves the alias, and the replicas of one rollout would run two
	// images. One made from another than the plan's is not started.

	switch {
	case r.target == "":
		r.target = made
	case made != r.target:
		return fmt.Errorf("%s was made from image %s, not %s as planned: the image's alias moved during the rollout", r.name(index), shortFingerprint(made), shortFingerprint(r.target))
	}

	// The healthcheck is waited for by the rollout, which also gives up on an
	// unhealthy verdict, and the dependencies are up already.
	return inst.Start(ctx, append(slices.Clone(r.startOpts), client.OptionNoHealthd())...)
}

// dependenciesHealthy reads the instances a new replica waits for on
// depends_on with service_healthy, and returns an error naming those that are
// not running and healthy.
func (r *rolloutRunner) dependenciesHealthy(ctx context.Context, inst *client.Instance) error {
	conn, err := r.c.Connection()
	if err != nil {
		return err
	}

	var errs error

	for _, name := range slices.Sorted(maps.Keys(inst.Config.Dependencies)) {
		if inst.Config.Dependencies[name] != client.HealthStatusHealthy {
			continue
		}

		dep, _, err := conn.GetInstance(ctx, r.c.IncusProject(), name, nil)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("dependency %s: %w", name, err))
			continue
		}

		status := dep.Config[shared.HealthStatusKey]
		if !rolloutRunning(dep.StatusCode) || status != shared.HealthStatusHealthy {
			if status == "" {
				status = "no healthcheck status"
			}

			errs = errors.Join(errs, fmt.Errorf("dependency %s is %s, %s: depends_on asks for it healthy", name, strings.ToLower(dep.StatusCode.String()), status))
		}
	}

	return errs
}

func (r *rolloutRunner) health(ctx context.Context, index int) (replicaRead, error) {
	conn, err := r.c.Connection()
	if err != nil {
		return replicaRead{}, err
	}

	inst, _, err := conn.GetInstance(ctx, r.c.IncusProject(), r.name(index), nil)
	if err != nil {
		return replicaRead{}, err
	}

	return replicaRead{
		ID:        inst.Config["volatile.uuid"],
		Status:    inst.Config[shared.HealthStatusKey],
		Running:   rolloutRunning(inst.StatusCode),
		Drained:   shared.DNSDrained(rolloutExpanded(&inst.Instance)[shared.DNSDrainKey]),
		DrainedBy: inst.Config[rolloutDrainKey],
	}, nil
}

// rolloutExpanded is the config ic-dns and ic-healthd go by, profiles
// included, or the instance's own when Incus gave no expanded one.
func rolloutExpanded(info *incusApi.Instance) map[string]string {
	if len(info.ExpandedConfig) > 0 {
		return info.ExpandedConfig
	}

	return info.Config
}

func (r *rolloutRunner) drain(ctx context.Context, index int, drained bool) error {
	keys := map[string]string{shared.DNSDrainKey: "", rolloutDrainKey: ""}
	if drained {
		keys = map[string]string{shared.DNSDrainKey: "true", rolloutDrainKey: r.runID}
	}

	return r.instances[index].SetConfigKeys(ctx, keys)
}

func (r *rolloutRunner) unmark(ctx context.Context, index int) error {
	return r.instances[index].SetConfigKeys(ctx, map[string]string{rolloutIDKey: "", rolloutPreStartKey: ""})
}

// sameInstance reads the replica and checks it is still the instance first
// read by its name, by volatile.uuid; nil, nil when there is none.
func (r *rolloutRunner) sameInstance(ctx context.Context, index int) (*incusApi.Instance, error) {
	conn, err := r.c.Connection()
	if err != nil {
		return nil, err
	}

	info, _, err := conn.GetInstance(ctx, r.c.IncusProject(), r.name(index), nil)
	if incusApi.StatusErrorCheck(err, http.StatusNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", r.name(index), err)
	}

	want := r.uuids[index]
	if want != "" && info.Config["volatile.uuid"] != want {
		return nil, fmt.Errorf("%s %w: left as it is", r.name(index), errRolloutReplaced)
	}

	return &info.Instance, nil
}

func (r *rolloutRunner) retire(ctx context.Context, index int, stopped bool, notAfter time.Time) error {
	readCtx, cancel := context.WithTimeout(ctx, rolloutReadTimeout)
	info, err := r.sameInstance(readCtx, index)

	cancel()

	if err != nil {
		return err
	}

	if info == nil {
		return client.ErrNotFound.WithKindName(client.KindInstance, r.name(index))
	}

	// Stopped once: started again by ic-healthd, it is the rollout's to read
	// the others again before a second stop (errRolloutRestarted).
	if !stopped {
		return r.takeDown(ctx, index, true, false, 1, notAfter)
	}

	// One read stopped serves nothing and was not drained: it is deleted only
	// while it is stopped, which Incus checks too, and never stopped here.
	if rolloutRunning(info.StatusCode) {
		return fmt.Errorf("%s started again since it was read stopped: not retired", r.name(index))
	}

	inst := r.instances[index]

	err = inst.SetHealthCheckingStopped(ctx, true)
	if err != nil {
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

	info, err := r.sameInstance(ctx, index)
	if err != nil || info == nil {
		return err
	}

	// Only what this run made is taken away: what its own create made, or
	// what carries its mark, made by the run picked up again. The name was
	// free when it was checked, but another process may have created an
	// instance by it in between, which the create then conflicted with.
	ours := inst.Created() || r.runID != "" && info.Config[rolloutIDKey] == r.runID
	if !ours {
		return fmt.Errorf("%s exists but was not created by this run: left as it is", r.name(index))
	}

	if !inst.IsEnsured() {
		err := inst.Ensure(ctx)
		if err != nil {
			return err
		}
	}

	// One kept for a look is let shut down; one going away is killed.
	return r.takeDown(ctx, index, keep, keep, rolloutTakeDownAttempts, time.Time{})
}

// takeDown stops a replica, gracefully within its grace period or not, and
// deletes it or, with keep, leaves it stopped.
//
// It is stopped up to attempts times when ic-healthd starts it again; past
// notAfter, when not zero, it is not stopped at all (errRolloutStale).
func (r *rolloutRunner) takeDown(ctx context.Context, index int, graceful bool, keep bool, attempts int, notAfter time.Time) error {
	inst := r.instances[index]

	// Each read is bounded, so a read that hangs does not hold up the stop's
	// deadline.
	status := func(ctx context.Context) (incusApi.StatusCode, error) {
		ctx, cancel := context.WithTimeout(ctx, 2*rolloutKillEvery)
		defer cancel()

		return r.status(ctx, index)
	}

	running := func(ctx context.Context) (bool, error) {
		s, err := status(ctx)

		return rolloutRunning(s), err
	}

	shutdown := func(ctx context.Context) error {
		return inst.Stop(ctx, client.OptionTimeout(r.stopTimeout))
	}

	kill := func(ctx context.Context) error {
		conn, err := r.c.Connection()
		if err != nil {
			return err
		}

		op, err := conn.UpdateInstanceState(ctx, r.c.IncusProject(), r.name(index), incusApi.InstanceStatePut{
			Action:  "stop",
			Force:   true,
			Timeout: -1,
		}, "")
		if err != nil {
			return err
		}

		_, err = iclient.WaitOperation(ctx, op)

		return err
	}

	bounded := func(ctx context.Context, call func(ctx context.Context) error) error {
		ctx, cancel := context.WithTimeout(ctx, rolloutReadTimeout)
		defer cancel()

		return call(ctx)
	}

	stop := func(ctx context.Context) error {
		// The marker keeps ic-healthd's restart policy off it from here on.
		err := bounded(ctx, func(ctx context.Context) error { return inst.SetHealthCheckingStopped(ctx, true) })
		if err != nil {
			return err
		}

		// The decision to stop it is good only for so long after the round
		// it was made on: past that, nothing is stopped, and it is made again.
		if !notAfter.IsZero() && time.Now().After(notAfter) {
			return errors.Join(errRolloutStale, bounded(ctx, func(ctx context.Context) error { return inst.SetHealthCheckingStopped(ctx, false) }))
		}

		return rolloutStop(ctx, shutdown, kill, status, graceful, rolloutStopSettle, time.Second, rolloutKillEvery)
	}

	remove := func(ctx context.Context) error {
		// Deleted by name, so only while the name is the instance's still.
		err := bounded(ctx, func(ctx context.Context) error {
			_, err := r.sameInstance(ctx, index)
			return err
		})
		if err != nil {
			return err
		}

		err = inst.Delete(ctx)
		if err == nil {
			delete(r.instances, index)
		}

		return err
	}

	startsAgain := func(ctx context.Context) (bool, error) {
		deadline := time.Now().Add(rolloutRestartWindow)

		// Stopped is what a read says, not the absence of one.
		var readErr error

		read := false

		for time.Now().Before(deadline) {
			err := r.sleep(ctx, time.Second)
			if err != nil {
				return false, err
			}

			again, err := running(ctx)
			if err != nil {
				readErr = err
				continue
			}

			read = true

			if again {
				return true, nil
			}
		}

		if !read {
			if readErr == nil {
				readErr = errors.New("no read in time")
			}

			return false, fmt.Errorf("%s not read stopped: %w", r.name(index), readErr)
		}

		return false, nil
	}

	return rolloutTakeDown(ctx, keep, attempts, stop, remove, running, startsAgain)
}

// rolloutRunning reports whether a status is one of a running instance. Incus
// reads an instance whose agent or init reported in as Ready, which runs and
// serves as one Running does: everything in a rollout that asks whether a
// replica runs goes by this.
func rolloutRunning(s incusApi.StatusCode) bool {
	return s == incusApi.Running || s == incusApi.Ready
}

// rolloutStop stops a replica and returns once Incus reads it Stopped. With
// graceful it is first asked to shut down within its grace period.
//
// What decides is the status read back, not what a stop call returned:
//   - the graceful stop and the client's deadline for it end together, and Incus
//     may still be carrying it out (a read of the instance halfway down fails
//     with "Invalid PID -1");
//   - a read during a stop can find the instance in Error for a moment, which
//     the client's Stop takes for "not running" and returns without stopping;
//   - ic-healthd's restart policy may have the instance in a stop and start of
//     its own.
//
// So it is killed whenever it reads running, again every killEvery while it
// does, and waited for in between, up to settle.
func rolloutStop(ctx context.Context, shutdown, kill func(ctx context.Context) error, status func(ctx context.Context) (incusApi.StatusCode, error), graceful bool, settle, poll, killEvery time.Duration) error {
	var errs []error

	if graceful {
		err := shutdown(ctx)
		if err != nil && !errors.Is(err, client.ErrNotRunning) {
			errs = append(errs, err)
		}
	}

	deadline := time.Now().Add(settle)

	// A kill that hangs on the API is bounded by the deadline, which it would
	// otherwise outlast: the transport waits up to an hour for an answer.
	killCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	var lastKill time.Time

	for {
		s, err := status(ctx)
		if err == nil && s == incusApi.Stopped {
			return nil
		}

		if err == nil && rolloutRunning(s) && (lastKill.IsZero() || time.Since(lastKill) >= killEvery) {
			lastKill = time.Now()

			killErr := kill(killCtx)
			if killErr != nil {
				errs = append(errs, killErr)
			}

			// A kill that took its time is read back at once, but it does
			// not put off the deadline: a status read after the last one
			// decides.
			if time.Now().Before(deadline) {
				continue
			}

			s, err = status(ctx)
			if err == nil && s == incusApi.Stopped {
				return nil
			}
		}

		if !time.Now().Before(deadline) {
			if err == nil {
				err = fmt.Errorf("still %s after %s", s, settle)
			}

			return errors.Join(append(errs, err)...)
		}

		t := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			t.Stop()

			return errors.Join(append(errs, ctx.Err())...)
		case <-t.C:
		}
	}
}

// rolloutTakeDown stops a replica for good and deletes it, or with keep leaves
// it stopped. ic-healthd's restart policy acts on the same unhealthy verdict a
// rollout gives up on, and on an old replica whose checks time out; a restart
// it began before the stop marker was written starts the replica again after
// the rollout stopped it. So it is stopped again, up to attempts times, until
// it is deleted or, kept, stays stopped.
func rolloutTakeDown(ctx context.Context, keep bool, attempts int, stop, remove func(ctx context.Context) error, running, startsAgain func(ctx context.Context) (bool, error)) error {
	for attempt := 1; ; attempt++ {
		err := stop(ctx)
		if err != nil {
			return err
		}

		if keep {
			again, err := startsAgain(ctx)
			if err != nil || !again {
				return err
			}
		} else {
			err = remove(ctx)
			if err == nil {
				return nil
			}

			again, readErr := running(ctx)
			if readErr != nil || !again {
				return err
			}
		}

		if attempt >= attempts {
			return fmt.Errorf("%w after %d stops, by ic-healthd's restart policy", errRolloutRestarted, attempt)
		}
	}
}

// status reads the replica's status code from Incus.
func (r *rolloutRunner) status(ctx context.Context, index int) (incusApi.StatusCode, error) {
	conn, err := r.c.Connection()
	if err != nil {
		return 0, err
	}

	info, _, err := conn.GetInstance(ctx, r.c.IncusProject(), r.name(index), nil)
	if err != nil {
		return 0, err
	}

	return info.StatusCode, nil
}

// instanceAbsent reports whether Incus has no instance by this name. Failing to
// read it is an error, not an answer.
func instanceAbsent(ctx context.Context, c *client.Client, name string) (bool, error) {
	conn, err := c.Connection()
	if err != nil {
		return false, err
	}

	_, _, err = conn.GetInstance(ctx, c.IncusProject(), name, nil)
	if err == nil {
		return false, nil
	}

	if incusApi.StatusErrorCheck(err, http.StatusNotFound) {
		return true, nil
	}

	return false, fmt.Errorf("reading %s: %w", name, err)
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
