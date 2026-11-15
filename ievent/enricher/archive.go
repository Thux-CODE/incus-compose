package enricher

import "github.com/lxc/incus-compose/ievent/iutil"

// archive is the last event emitted about each subject, so one saying nothing
// new can be stopped before it reaches the chain. Owned by the goroutine Run
// owns.
type archive map[string]*iutil.Event

// changed reports whether ev says anything the last event about its subject did
// not, and files it either way.
//
// A read confirming what was restored from the cold store says something new
// even where the instance is the same: that it is no longer a restored answer.
//
// Only an instance event reaches here: a call carries events and one of its own
// on the instance path alone, and a network read has neither.
func (a archive) changed(ev *iutil.Event) bool {
	key := resourceKey(kindInstance, ev.ProjectName(), ev.Name())

	was := a[key]
	a[key] = ev

	if was != nil && was.Restored() != ev.Restored() {
		return true
	}

	return !ev.Equal(was)
}

// forget drops what was last said about one instance, so a name that comes back
// is news again.
func (a archive) forget(project, name string) {
	delete(a, resourceKey(kindInstance, project, name))
}
