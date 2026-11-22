package enricher

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"maps"
	"net/netip"
	"os"
	"slices"
	"strings"

	incusapi "github.com/lxc/incus/v7/shared/api"

	"github.com/lxc/incus-compose/ievent/iutil"
)

// newNetwork cuts one network down to what a record needs of it. Incus reports
// the owning project on every listing, so the project is known here rather than
// looked up in a map that could be older than the network.
func newNetwork(n incusapi.Network) *iutil.Network {
	// Unmanaged still keys records: two instances on one unmanaged network can
	// reach each other, and it has no subnet of its own to serve.
	if !n.Managed {
		return iutil.NewNetwork(n.Name, n.Project, false, "", "")
	}

	return iutil.NewNetwork(n.Name, n.Project, true,
		subnet(n.Config["ipv4.address"]),
		subnet(n.Config["ipv6.address"]))
}

// subnet is one configured address, or empty for the sentinels Incus writes
// where there is no subnet to serve: none, and an auto it has not filled in.
func subnet(address string) string {
	if address == "none" || address == "auto" {
		return ""
	}

	return address
}

// project is one project's worth of the fleet. Nested rather than flat maps
// keyed by project, so deleting a project takes what it held with it.
type project struct {
	config    map[string]string
	instances map[string]*iutil.Instance
	networks  map[string]*iutil.Network

	// listed is a listing of its instances having succeeded, in this process
	// or the one that wrote the cold store: what a listing that keeps failing
	// has to answer from. A project made again under the same name starts
	// without it, since the old one went with what it held.
	listed bool
}

// newProject is a project something has been said about. The config stays nil
// until a read fills it: an empty map would make a project that sets nothing
// look like one nothing has read, and withProject turns on that difference.
func newProject() *project {
	return &project{
		instances: map[string]*iutil.Instance{},
		networks:  map[string]*iutil.Network{},
	}
}

// state is the fleet as the enricher last saw it.
type state struct {
	projects map[string]*project

	// projectsListed is a listing of the projects having succeeded, here or
	// in the run that wrote the cold store, empty included. With each
	// project's listed it is the inventory: known only from a listing, never
	// from what one read or one event brought in.
	projectsListed bool

	// dirty says something has changed since the last clone went to the store.
	// Set by every write here, cleared by the fold when it takes one.
	dirty bool
}

// newState prepares an empty state, which is what a start with no cold store
// to read begins from. See loadState for one that has.
func newState() *state {
	return &state{
		projects: map[string]*project{},
	}
}

// clone is the state as it stands, for a goroutine that may not touch the live
// one.
//
// The instances and networks are shared rather than copied: nothing ever writes
// to one, they are replaced whole, so a reader holding this sees whatever was
// there when it was taken. What a project sets is a plain map and is copied,
// which is the one thing here that could move under a reader.
func (s *state) clone() *state {
	out := &state{projects: make(map[string]*project, len(s.projects))}

	for name, p := range s.projects {
		out.projects[name] = &project{
			config:    maps.Clone(p.config),
			instances: maps.Clone(p.instances),
			networks:  maps.Clone(p.networks),
			listed:    p.listed,
		}
	}

	out.projectsListed = s.projectsListed

	return out
}

// stateJSON and projectJSON are the wire form of the fleet. Their own types
// rather than tags on the fields, so the file format is one visible thing and
// renaming a field here is not a format change.
//
// Version is absent from what v1.4.0 wrote, which is the same shape as version
// one: a missing key reads as zero and is taken for it. Version two adds what
// was listed, written whatever it is: in it, a project whose instances were
// not listed is one, and so are the projects. A version one file never said,
// and a project holding instances is taken as listed there, as the projects
// are where one is. Anything else is a format this binary does not know, and
// is ignored rather than migrated.
type stateJSON struct {
	Version        int                    `json:"version"`
	Projects       map[string]projectJSON `json:"projects"`
	ProjectsListed bool                   `json:"projects_listed,omitempty"`
}

// storeVersion is the on-disk format written here.
const storeVersion = 2

type projectJSON struct {
	// Not omitempty: a project that sets nothing writes an empty object, and one
	// nothing has read writes null. Dropping the key would make them one, and a
	// reload would take a project it had read for one it had not.
	Config    map[string]string          `json:"config"`
	Instances map[string]*iutil.Instance `json:"instances,omitempty"`
	Networks  map[string]*iutil.Network  `json:"networks,omitempty"`

	// Listed is the project's instances having been listed: an empty project
	// listed is an inventory, one whose configuration alone was read, or one
	// instance an event brought in, is not.
	Listed bool `json:"listed,omitempty"`
}

func (s state) MarshalJSON() ([]byte, error) {
	out := stateJSON{Version: storeVersion, Projects: make(map[string]projectJSON, len(s.projects)), ProjectsListed: s.projectsListed}

	for projectName, p := range s.projects {
		out.Projects[projectName] = projectJSON{
			Config:    p.config,
			Instances: p.instances,
			Networks:  p.networks,
			Listed:    p.listed,
		}
	}

	return json.Marshal(out)
}

// errStoreVersion is a file written in a format this binary does not read.
var errStoreVersion = errors.New("unknown cold store version")

// decodeState reads what a previous run wrote back into a state, as it was
// written: which projects are still served is loadState's question.
func decodeState(b []byte) (*state, error) {
	var in stateJSON

	err := json.Unmarshal(b, &in)
	if err != nil {
		return nil, err
	}

	if in.Version < 0 || in.Version > storeVersion {
		return nil, fmt.Errorf("%w %d", errStoreVersion, in.Version)
	}

	legacy := in.Version < storeVersion

	out := newState()
	out.projectsListed = in.ProjectsListed

	for projectName, p := range in.Projects {
		held := newProject()
		held.config = p.Config

		for instanceName, i := range p.Instances {
			// A null written for an instance is no instance: a nil here would
			// reach every reader that ranges over its interfaces.
			if i != nil {
				held.instances[instanceName] = i
			}
		}

		for networkName, n := range p.Networks {
			if n != nil {
				held.networks[networkName] = n
			}
		}

		held.listed = p.Listed || (legacy && len(held.instances) > 0)
		out.projectsListed = out.projectsListed || (legacy && held.listed)

		out.projects[projectName] = held
	}

	return out, nil
}

// loadState is the fleet a previous run of this process left in path, cut down
// to what the configuration of this one serves - or an empty state, which is
// what a missing, unreadable or unknown file all come to. Never an error: this
// is a cache, and starting without it costs only the wait for the first run.
//
// A project the predicate no longer serves - the list or the marker changed
// between the two starts - keeps its networks, since an instance in a served
// project may sit on one, and loses its instances and what it sets. One whose
// configuration was never read is asked with none: a list of names still
// answers for it, and a marker it cannot be seen to carry does not. A project
// the certificate no longer sees cannot be told from here; the first listing of
// the run prunes it, and the prune announces every instance it held.
func loadState(logger *slog.Logger, path string, serves func(*incusapi.Project) bool) *state {
	if path == "" {
		return newState()
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			logger.Info("no cold store yet, starting cold", "plugin", name, "path", path)
		} else {
			logger.Warn("the cold store could not be read, starting cold",
				"plugin", name, "path", path, "err", err)
		}

		return newState()
	}

	s, err := decodeState(b)
	if err != nil {
		logger.Warn("the cold store could not be decoded, starting cold",
			"plugin", name, "path", path, "err", err)

		return newState()
	}

	if serves != nil {
		for projectName, p := range s.projects {
			config := maps.Clone(p.config)
			if config == nil {
				config = map[string]string{}
			}

			if serves(&incusapi.Project{Name: projectName, ProjectPut: incusapi.ProjectPut{Config: config}}) {
				continue
			}

			if len(p.networks) == 0 {
				delete(s.projects, projectName)

				continue
			}

			p.config = nil
			p.instances = map[string]*iutil.Instance{}
			p.listed = false
		}
	}

	logger.Info("restored the fleet from the cold store",
		"plugin", name, "path", path,
		"projects", s.projectCount(), "instances", s.instanceCount())

	return s
}

// restored is every instance held, in a fixed order, which is what a start
// replays before the first run has read any of it.
func (s *state) restored() []subject {
	var out []subject

	for projectName, p := range s.projects {
		for instanceName := range p.instances {
			out = append(out, subject{project: projectName, instance: instanceName})
		}
	}

	slices.SortFunc(out, func(a, b subject) int {
		return strings.Compare(a.project+"/"+a.instance, b.project+"/"+b.instance)
	})

	return out
}

// resourceKey identifies one resource by its kind, and by the project that owns
// it.
//
// Every bridge lives in the default project and other projects reference it, so
// a bridge two projects both use is one entry. The project has to be part of it
// because two projects with features.networks may each own the same name, and
// the kind because a network and an instance may share one.
func resourceKey(kind, project, name string) string {
	return kind + "/" + project + "/" + name
}

// project is one project, created if this is the first thing said about it.
// Only a write path may call this: a read that created one would make a project
// nothing has read indistinguishable from one that is simply empty.
func (s *state) project(n string) *project {
	p, ok := s.projects[n]
	if !ok {
		p = newProject()
		s.projects[n] = p
	}

	return p
}

// setNetwork patches one network in.
func (s *state) setNetwork(n incusapi.Network) {
	s.dirty = true

	s.project(n.Project).networks[n.Name] = newNetwork(n)
}

// deleteNetwork removes one network. The instances on it keep their addresses:
// what went is the network, and the next read of each says what that means.
func (s *state) deleteNetwork(project, name string) {
	s.dirty = true

	p, ok := s.projects[project]
	if !ok {
		return
	}

	delete(p.networks, name)
}

// network answers the network one NIC names, and which project owns it.
//
// A bare name resolves in the instance's own project first and the default
// project's second, which is what Incus does without features.networks.
func (s *state) network(project, name string) (*iutil.Network, string, bool) {
	for _, project := range [2]string{project, iutil.DefaultProject} {
		p, ok := s.projects[project]
		if !ok {
			continue
		}

		w, ok := p.networks[name]
		if ok {
			return w, project, true
		}
	}

	return nil, "", false
}

// setProject records a project's own configuration, as it was read.
func (s *state) setProject(name string, config map[string]string) {
	s.dirty = true

	s.project(name).config = config
}

// setListed records a listing that succeeded: of one project's instances, or
// with project empty, of the projects. Durable at once: dirty, so the next
// clone carries it even where nothing else changed - an empty listing changes
// nothing else.
func (s *state) setListed(project string) {
	s.dirty = true

	if project == "" {
		s.projectsListed = true

		return
	}

	s.project(project).listed = true
}

// listed reports whether one project's instances have been listed, here or by
// the run that wrote the cold store, since the project was last made.
func (s *state) listed(name string) bool {
	p, ok := s.projects[name]

	return ok && p.listed
}

// projectConfig is one project's own configuration, or nil where nothing has
// read it. Ask hasProject to tell that from a project that sets nothing.
func (s *state) projectConfig(name string) map[string]string {
	p, ok := s.projects[name]
	if !ok {
		return nil
	}

	return p.config
}

// deleteProject removes a project and everything in it. A project cannot be
// deleted while it holds instances, but a project we stop serving takes its
// instances out of the state with it either way.
func (s *state) deleteProject(name string) {
	s.dirty = true

	delete(s.projects, name)
}

// setInstance cuts one instance read down and patches it in, or answers nil.
//
// An incomplete read is no read: a NIC naming a network nothing has read yet
// cannot be placed, so nothing is stored and the next read is compared against
// the last whole answer there was. The second answer is the network that was
// missing, which is what has to be read for the next attempt to place it.
func (s *state) setInstance(
	inst *incusapi.Instance,
	instanceState *incusapi.InstanceState,
) (*iutil.Instance, string) {
	i, missing := s.buildInstance(inst, instanceState)
	if i == nil {
		return nil, missing
	}

	s.dirty = true
	s.project(inst.Project).instances[inst.Name] = i

	return i, ""
}

// buildInstance is what setInstance would hold for one read, held nowhere:
// nil and the network that was missing where it could not be placed.
func (s *state) buildInstance(
	inst *incusapi.Instance,
	instanceState *incusapi.InstanceState,
) (*iutil.Instance, string) {
	interfaces, networks, missing, whole := s.interfaces(inst, instanceState)
	if !whole {
		return nil, missing
	}

	config := inst.Config
	if len(inst.ExpandedConfig) > 0 {
		config = inst.ExpandedConfig
	}

	held := map[string]string{}

	for k, v := range config {
		if !strings.HasPrefix(k, "volatile.") {
			held[k] = v
		}
	}

	// Ready is Running with the guest's own word that it is up (PATCH
	// /dev/incus), not a state of its own: a consumer telling a paused or
	// stopped instance from a running one has to see it as running.
	running := inst.StatusCode == incusapi.Running || inst.StatusCode == incusapi.Ready

	return iutil.NewInstance(running, held, interfaces, networks), ""
}

// interfaces is where one instance sits: one entry per NIC whose network is
// known, the networks those NICs name, whether the whole of it could be placed,
// and where it could not, the network that was missing.
//
// The networks travel with the instance because nothing downstream can work out
// a subnet from a name, and the subnet is what places a querier.
func (s *state) interfaces(
	inst *incusapi.Instance,
	instanceState *incusapi.InstanceState,
) (found []iutil.InstanceInterface, networks map[string]*iutil.Network, missing string, whole bool) {
	// Nothing to read the addresses off: a stopped instance sits nowhere, which
	// is a whole answer rather than a missing one.
	if instanceState == nil {
		return nil, nil, "", true
	}

	// Expanded first, so NICs supplied by a profile are seen; a compose stack
	// puts most of its NICs on one.
	devices := inst.ExpandedDevices
	if len(devices) == 0 {
		devices = inst.Devices
	}

	networks = map[string]*iutil.Network{}

	// The state keys an interface by its name in the guest, which is the
	// device key only by default. A profile may call the device anything -
	// "eth-1", or the network's own name - while the guest still sees eth0, so
	// the device is found by what Incus records about it rather than by name.
	byHwaddr, byName := nicIndex(inst, devices)

	for ifname, iface := range instanceState.Network {
		if iface.Type == "loopback" {
			continue
		}

		device := nicDevice(ifname, iface, byHwaddr, byName)

		// A NIC on an unmanaged host bridge carries parent instead of network,
		// with no network key at all - both are valid device shapes.
		network := devices[device]["network"]
		if network == "" {
			network = devices[device]["parent"]
		}

		if network == "" {
			continue
		}

		w, project, known := s.network(inst.Project, network)

		// An instance is only ever created on a network that already exists, so
		// this read overtook the network event rather than found a real gap.
		if !known {
			return nil, nil, network, false
		}

		networks[iutil.NetworkKey(project, w.Name())] = w

		v4 := addresses(iface, netip.Addr.Is4)
		if len(v4) == 0 {
			v4 = parseIP(devices[device]["ipv4.address"], netip.Addr.Is4)
		}

		v6 := addresses(iface, netip.Addr.Is6)
		if len(v6) == 0 {
			v6 = parseIP(devices[device]["ipv6.address"], netip.Addr.Is6)
		}

		found = append(found, iutil.NewInstanceInterface(project, w.Name(), w.Managed(), v4, v6))
	}

	// Sorted, so two reads of one instance compare equal whatever order the
	// daemon happened to list its NICs in.
	slices.SortFunc(found, func(a, b iutil.InstanceInterface) int {
		return strings.Compare(
			iutil.NetworkKey(a.Project(), a.Network()),
			iutil.NetworkKey(b.Project(), b.Network()))
	})

	return found, networks, "", true
}

// nicIndex maps what the state reports about an interface - its hardware
// address, and its name in the guest - back to the key its device has in the
// configuration. Incus records both on the instance as volatile.<device>.hwaddr
// and volatile.<device>.name once the NIC is up; a device may also pin either
// itself. The key is the last resort, and the only one when nothing is
// recorded, which is what a device that has never been started looks like.
func nicIndex(
	inst *incusapi.Instance,
	devices map[string]map[string]string,
) (byHwaddr, byName map[string]string) {
	config := inst.ExpandedConfig
	if len(config) == 0 {
		config = inst.Config
	}

	byHwaddr = map[string]string{}
	byName = map[string]string{}

	for key, device := range devices {
		if t := device["type"]; t != "" && t != "nic" {
			continue
		}

		for _, hwaddr := range []string{config["volatile."+key+".hwaddr"], device["hwaddr"]} {
			if hwaddr != "" {
				byHwaddr[strings.ToLower(hwaddr)] = key
			}
		}

		name := device["name"]
		if name == "" {
			name = config["volatile."+key+".name"]
		}

		if name == "" {
			name = key
		}

		byName[name] = key
	}

	return byHwaddr, byName
}

// nicDevice is the device key behind one interface of the state. The hardware
// address is the surest link, since the guest may rename an interface; the
// name Incus gave it comes next; the interface name itself is what is left.
func nicDevice(
	ifname string,
	iface incusapi.InstanceStateNetwork,
	byHwaddr, byName map[string]string,
) string {
	if device, ok := byHwaddr[strings.ToLower(iface.Hwaddr)]; ok && iface.Hwaddr != "" {
		return device
	}

	if device, ok := byName[ifname]; ok {
		return device
	}

	return ifname
}

// addresses is every global address of one family the interface holds. Anything
// scoped below global is skipped: it reaches nobody off the host.
func addresses(iface incusapi.InstanceStateNetwork, family func(netip.Addr) bool) []string {
	out := []string{}

	for _, address := range iface.Addresses {
		if address.Scope != "global" {
			continue
		}

		ip, err := netip.ParseAddr(address.Address)
		if err != nil {
			continue
		}

		if family(ip) {
			out = append(out, address.Address)
		}
	}

	slices.Sort(out)

	return out
}

// parseIP parses a static IP address or CIDR notation into an address string if
// it matches the family.
func parseIP(raw string, family func(netip.Addr) bool) []string {
	if raw == "" {
		return nil
	}

	addr, err := netip.ParseAddr(raw)
	if err == nil {
		if family(addr) {
			return []string{addr.String()}
		}

		return nil
	}

	prefix, err := netip.ParsePrefix(raw)
	if err != nil {
		return nil
	}

	if family(prefix.Addr()) {
		return []string{prefix.Addr().String()}
	}

	return nil
}

// deleteInstance removes one instance. This is what a delete does, and half of
// what a rename does - the other half is a read of the new name.
func (s *state) deleteInstance(project, name string) {
	s.dirty = true

	p, ok := s.projects[project]
	if !ok {
		return
	}

	delete(p.instances, name)
}

func (s *state) projectCount() int {
	return len(s.projects)
}

func (s *state) instanceCount() int {
	total := 0
	for _, p := range s.projects {
		total += len(p.instances)
	}

	return total
}

// instance returns what is held for one instance, or nil.
func (s *state) instance(project, name string) *iutil.Instance {
	p, ok := s.projects[project]
	if !ok {
		return nil
	}

	return p.instances[name]
}

// missing is every name held in one scope that a listing does not have. A
// listing is the whole of its scope, so what it left out has gone.
func missing[T any](held map[string]T, listed []string) []string {
	has := make(map[string]bool, len(listed))

	for _, name := range listed {
		has[name] = true
	}

	var out []string

	for name := range held {
		if !has[name] {
			out = append(out, name)
		}
	}

	slices.Sort(out)

	return out
}

// missingInstances is every instance of one project a listing left out.
//
// Answered rather than deleted: each has to go the way an announced delete does,
// through the event that drops it from the archive as well as from here.
func (s *state) missingInstances(project string, listed []string) []subject {
	p, ok := s.projects[project]
	if !ok {
		return nil
	}

	var out []subject

	for _, name := range missing(p.instances, listed) {
		out = append(out, subject{project: project, instance: name})
	}

	return out
}

// pruneNetworks drops every network of one project a listing left out.
func (s *state) pruneNetworks(project string, listed []string) {
	s.dirty = true

	p, ok := s.projects[project]
	if !ok {
		return
	}

	for _, name := range missing(p.networks, listed) {
		delete(p.networks, name)
	}
}

// missingProjects is every instance held in a project a listing left out.
//
// Answered rather than deleted, for the reason missingInstances is: a project
// that went, or one this binary no longer serves, took its instances with it,
// and each has to reach the chain as a delete or it answers for ever.
func (s *state) missingProjects(listed []string) []subject {
	var out []subject

	for _, projectName := range missing(s.projects, listed) {
		names := slices.Sorted(maps.Keys(s.projects[projectName].instances))

		for _, instanceName := range names {
			out = append(out, subject{project: projectName, instance: instanceName})
		}
	}

	return out
}

// pruneProjects drops every project a listing left out, and everything in one.
func (s *state) pruneProjects(listed []string) {
	s.dirty = true

	for _, name := range missing(s.projects, listed) {
		delete(s.projects, name)
	}
}

// subject is one instance, named the way an event names it.
type subject struct {
	project  string
	instance string
}

// networkInstances is every instance the state holds that sits on one network,
// which is what a network change fans out over - answered without a read,
// because where each instance sits was worked out when it was read.
func (s *state) networkInstances(project, name string) iter.Seq[subject] {
	return func(yield func(subject) bool) {
		for projectName, p := range s.projects {
			for instanceName, i := range p.instances {
				if !on(i, project, name) {
					continue
				}

				if !yield(subject{project: projectName, instance: instanceName}) {
					return
				}
			}
		}
	}
}

// addressed reports whether the instance holds an address anywhere. A NIC that
// is up before its lease keys a network with nothing on it, so where it sits
// does not say on its own.
func addressed(i *iutil.Instance) bool {
	for iface := range i.Interfaces() {
		if len(iface.IPv4()) > 0 || len(iface.IPv6()) > 0 {
			return true
		}
	}

	return false
}

// on reports whether one instance sits on the named network. Both halves are
// compared: two projects with features.networks may own the same bare name.
func on(i *iutil.Instance, project, name string) bool {
	for iface := range i.Interfaces() {
		if iface.Project() == project && iface.Network() == name {
			return true
		}
	}

	return false
}

// projectInstances is every instance name the state holds for one project, which
// is what a profile change fans out over without reading the project to find out.
func (s *state) projectInstances(project string) iter.Seq[subject] {
	return func(yield func(subject) bool) {
		p, ok := s.projects[project]
		if !ok {
			return
		}

		for i := range p.instances {
			if !yield(subject{project: project, instance: i}) {
				return
			}
		}
	}
}
