package project

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/types"
	incusApi "github.com/lxc/incus/v7/shared/api"
	"github.com/lxc/incus/v7/shared/util"

	"github.com/lxc/incus-compose/client"
	"github.com/lxc/incus-compose/shared"
)

// replicaPlan is how Resources builds one service.
type replicaPlan struct {
	// indexes holds every replica built, ascending, the dropped ones included.
	indexes []int
	drop    map[int]bool
	targets map[int]string

	// first is the replica the service-wide pre_start hooks and the DNS
	// aliases go to, 0 for none.
	first int
}

// replicaPlans decides, per service, the replicas Resources builds. A service
// is reconciled on the set of its replicas that exist, not on the names 1 to
// N: the same count is left as it is, whatever the indexes.
func (p *Project) replicaPlans(c *client.Client, options *ResourcesOptions) (map[string]replicaPlan, error) {
	instances, err := c.Instances()
	if err != nil {
		return nil, err
	}

	for _, name := range options.recreating {
		listed := slices.ContainsFunc(instances, func(i incusApi.InstanceFull) bool { return i.Name == name })
		if !listed {
			instances = append(instances, incusApi.InstanceFull{Instance: incusApi.Instance{
				Name:        name,
				StatusCode:  incusApi.Running,
				InstancePut: incusApi.InstancePut{Config: map[string]string{recreatingKey: "true"}},
			}})
		}
	}

	clustered := c.Global().IsClustered()

	var serverName string
	if clustered {
		serverName = c.Global().ServerName()
	}

	plans := map[string]replicaPlan{}

	var errs error

	for _, service := range p.Services {
		// A one-off is not one of the service's replicas, so it reconciles nothing.
		if options.oneOff != nil && options.oneOff.Service == service.Name {
			plans[service.Name] = replicaPlan{indexes: []int{1}, first: 1, targets: map[int]string{1: serverName}}

			continue
		}

		hash, err := ServiceConfigHash(service)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		existing := serviceReplicas(instances, p.Name, service.Name, hash)
		markImage(existing, serviceImageTarget(c, service))

		add := options.addReplica
		if add != nil && add.service != service.Name {
			add = nil
		}

		// CLI --scale > deploy.replicas / deploy.mode: global > 1. A plain `up`
		// reconciles to deploy.replicas in both directions, matching `docker
		// compose up`: a manual --scale applies only to that invocation, and the
		// next plain `up` restores replicas (scaling up or down).
		var scaleOverride *int

		s, ok := options.Scale[service.Name]
		if ok {
			scaleOverride = &s
		}

		if add != nil {
			s := len(existing) + 1
			scaleOverride = &s
		}

		desired, placement, err := p.resolveServicePlacement(c, service, scaleOverride)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		// A container_name is one name, or one per index when scaled: there is
		// no set of replicas to reconcile, only the declared count.
		if service.ContainerName != "" {
			plan := replicaPlan{first: 1, targets: map[int]string{}}

			for i := 1; i <= desired; i++ {
				plan.indexes = append(plan.indexes, i)

				if i <= len(placement) {
					plan.targets[i] = placement[i-1]
				} else {
					plan.targets[i] = serverName
				}
			}

			plans[service.Name] = plan

			continue
		}

		var plan replicaPlan

		if add != nil {
			for _, r := range existing {
				plan.indexes = append(plan.indexes, r.index)
			}

			plan.indexes = append(plan.indexes, add.index)
			slices.Sort(plan.indexes)

			if add.first {
				plan.first = add.index
			}
		} else {
			plan.indexes, plan.drop = reconcileReplicas(existing, desired)

			for _, i := range plan.indexes {
				if !plan.drop[i] {
					plan.first = i
					break
				}
			}
		}

		plan.targets = replicaTargets(plan.indexes, plan.drop, existing, placement, clustered, serverName)
		plans[service.Name] = plan
	}

	return plans, errs
}

// serviceImageTarget returns the fingerprint the service's image points at,
// as this client ensured it already (on the build that follows the pull of
// `up`), and "" when it has not.
func serviceImageTarget(c *client.Client, service types.ServiceConfig) string {
	name := service.Image
	if service.Build != nil && name == "" {
		name = "localhost/" + service.Name
	}

	if name == "" {
		return ""
	}

	res, err := c.Resource(client.KindImage, name, nil)
	if err != nil {
		return ""
	}

	img, ok := res.(*client.Image)
	if !ok || !img.IsEnsured() {
		return ""
	}

	return img.State().IncusAlias.Target
}

// ReplicaIndexes returns the indexes of the replicas of a service that exist,
// ascending, whatever they are.
func (p *Project) ReplicaIndexes(c *client.Client, service string) ([]int, error) {
	instances, err := c.Instances()
	if err != nil {
		return nil, err
	}

	var out []int
	for _, r := range serviceReplicas(instances, p.Name, service, "") {
		out = append(out, r.index)
	}

	return out, nil
}

// replica is an instance of a service that exists, as reconciling sees it.
type replica struct {
	index    int
	location string
	running  bool
	drained  bool

	// current is made from the service config as it is now, or carries no
	// hash to tell; unhashed is the one with no hash.
	current  bool
	unhashed bool

	// baseImage is the image it was made from, and otherImage tells it is
	// not the one the service's image points at now, when that is known
	// (markImage).
	baseImage  string
	otherImage bool

	// healthy runs, is not drained and was called healthy, or has no
	// healthcheck to call it anything.
	healthy bool
}

// markImage tells the replicas made from another image than target, the
// fingerprint the service's image points at now; with none known, none is.
func markImage(existing []replica, target string) {
	for i := range existing {
		existing[i].otherImage = target != "" && existing[i].baseImage != target
	}
}

// healthWatched reports whether ic-healthd runs a check on an instance with
// this config: enabled, with a test that is not compose's NONE. Such a replica
// serves only once a check called it healthy.
func healthWatched(cfg map[string]string) bool {
	if !util.IsTrue(cfg[shared.HealthEnabledKey]) {
		return false
	}

	raw := strings.TrimSpace(cfg[shared.HealthKeyPrefix+"test"])
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

// recreatingKey marks the stand-in replicaPlans lists for an instance being
// recreated, deleted already: it was one of the service's, labels and all. It
// is never written to Incus.
const recreatingKey = "incus-compose.recreating"

// serviceReplicas picks the replicas of a service out of the instances of a
// project: named `<service>-<index>`, and labeled with the service and the
// compose project. What reconciling finds is what it may delete, so an
// instance without the labels is never one: made by hand next to the replicas,
// or by an incus-compose older than the labels. Such an old one keeps its name
// through the declared names, which an `up` gives the labels to. One-offs and
// pre_start runners are not replicas.
func serviceReplicas(instances []incusApi.InstanceFull, project, service, hash string) []replica {
	var out []replica

	for _, inst := range instances {
		cfg := inst.Config
		if util.IsTrue(cfg[OneOffKey]) {
			continue
		}

		owned := cfg[ServiceLabelKey] == service && cfg[labelIncusComposePrefix+"project"] == project
		if !owned && !util.IsTrue(cfg[recreatingKey]) {
			continue
		}

		index := replicaIndex(inst.Name, service)
		if index < 1 {
			continue
		}

		running := inst.StatusCode == incusApi.Running || inst.StatusCode == incusApi.Ready
		drained := util.IsTrue(cfg[shared.DNSDrainKey])
		status := cfg[shared.HealthStatusKey]
		instHash, hashed := cfg[client.ConfigHashKey]

		// One with a check that has not reached a verdict yet, just started
		// or made by a client killed before ic-healthd wrote one, does not
		// serve: it covers the removal of no other.
		verdict := status == shared.HealthStatusHealthy || status == "" && !healthWatched(cfg)

		out = append(out, replica{
			index:     index,
			location:  inst.Location,
			running:   running,
			drained:   drained,
			current:   !hashed || instHash == hash,
			unhashed:  !hashed,
			baseImage: cfg["volatile.base_image"],
			healthy:   running && !drained && verdict,
		})
	}

	slices.SortFunc(out, func(a, b replica) int { return a.index - b.index })

	return out
}

// replicaHashedIndexes is how far replicaIndex looks for the index of a
// replica whose name is a hash.
const replicaHashedIndexes = 1024

// replicaIndex returns the index of the replica of service named name, 0 when
// it is not named as one. A name too long for Incus is a hash of it
// (client.SanitizeIncusName), with no index to read, so the index is the one
// whose name hashes to it.
func replicaIndex(name, service string) int {
	nameOf := func(index int) string {
		return client.SanitizeIncusName(fmt.Sprintf("%s-%d", service, index), -1)
	}

	dash := strings.LastIndexByte(name, '-')
	if dash >= 0 {
		index, err := strconv.Atoi(name[dash+1:])
		if err == nil && index >= 1 && nameOf(index) == name {
			return index
		}
	}

	if len(name) != 32 {
		return 0
	}

	for index := 1; index <= replicaHashedIndexes; index++ {
		if nameOf(index) == name {
			return index
		}
	}

	return 0
}

// reconcileReplicas decides the indexes a service is built with to have
// desired replicas, out of the ones that exist, whatever their indexes. Fewer
// than desired are joined by new ones on the lowest free indexes. More give
// up the surplus, which drop holds: first the ones not running, then the
// drained ones, then the ones made from an older service config, then the ones
// with no config hash, then the ones made from another image than the
// service's, then the highest index. The ones kept hold as many healthy
// replicas as there are, up to desired: a healthy one goes only for another
// healthy one.
//
// The indexes are ascending, and include the ones dropped, which have to be
// built to be deleted.
func reconcileReplicas(existing []replica, desired int) ([]int, map[int]bool) {
	indexes := make([]int, 0, max(len(existing), desired))
	for _, r := range existing {
		indexes = append(indexes, r.index)
	}

	if len(existing) <= desired {
		for i := 1; len(indexes) < desired; i++ {
			if !slices.Contains(indexes, i) {
				indexes = append(indexes, i)
			}
		}

		slices.Sort(indexes)

		return indexes, nil
	}

	rank := func(r replica) int {
		switch {
		case !r.running:
			return 0
		case r.drained:
			return 1
		case !r.current:
			return 2
		case r.unhashed:
			return 3
		case r.otherImage:
			return 4
		default:
			return 5
		}
	}

	// In the order they go.
	order := slices.Clone(existing)
	slices.SortStableFunc(order, func(a, b replica) int {
		return cmp.Or(rank(a)-rank(b), b.index-a.index)
	})

	surplus := len(existing) - desired

	healthy := func(rs []replica) int {
		n := 0
		for _, r := range rs {
			if r.healthy {
				n++
			}
		}

		return n
	}

	// The healthy one that would go last swaps with the first one kept that
	// is not, until the ones kept hold as many as there are, up to desired.
	need := min(desired, healthy(existing))

	for healthy(order[surplus:]) < need {
		j := slices.IndexFunc(order[surplus:], func(r replica) bool { return !r.healthy }) + surplus

		for k := surplus - 1; k >= 0; k-- {
			if order[k].healthy {
				order[k], order[j] = order[j], order[k]

				break
			}
		}
	}

	drop := map[int]bool{}
	for _, r := range order[:surplus] {
		drop[r.index] = true
	}

	slices.Sort(indexes)

	return indexes, drop
}

// replicaTargets places the replicas a service is built with on a cluster: one
// that exists stays where it runs, and a new one takes a target of the
// placement that no kept replica holds already, in index order. Outside a
// cluster there is no target.
func replicaTargets(indexes []int, drop map[int]bool, existing []replica, placement []string, clustered bool, fallback string) map[int]string {
	out := map[int]string{}
	if !clustered {
		return out
	}

	located := map[int]string{}
	for _, r := range existing {
		if r.location != "" {
			located[r.index] = r.location
		}
	}

	free := slices.Clone(placement)

	for _, i := range indexes {
		loc, ok := located[i]
		if !ok || drop[i] {
			continue
		}

		at := slices.Index(free, loc)
		if at >= 0 {
			free = slices.Delete(free, at, at+1)
		}
	}

	for _, i := range indexes {
		loc, ok := located[i]

		switch {
		case ok:
			out[i] = loc
		case len(free) > 0:
			out[i] = free[0]
			free = free[1:]
		default:
			out[i] = fallback
		}
	}

	return out
}
