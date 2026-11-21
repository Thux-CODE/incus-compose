---
date: 2026-11-15T00:00:00Z
dateCreated: 2026-11-15T00:00:00Z
tags: []
---

# rollout

Replace the replicas of a service one at a time, new before old: the old replica
keeps serving until the one that takes its place is healthy. If the new one does
not get there, it is taken away and the old one is left as it was. This works
for a service with a single replica too, which is the case it is made for: an
application that takes minutes to start.

```
incus-compose rollout [options] SERVICE...
```

| Option               | Description                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| `--pull`             | `missing` (default), `always` or `never`, as for `up`                                           |
| `--timeout`          | Timeout for creating and starting a new replica, before its healthcheck (default: 2m)           |
| `--health-timeout`   | How long a new replica has to become healthy (default: derived from the healthcheck, see below) |
| `--dns-ttl`          | TTL the project's ic-dns serves records with (default: 5s, ic-dns' own default)                 |
| `--drain-delay`      | Extra time a drained replica keeps running, for requests in flight (default: 5s)                |
| `--stop-timeout`     | Timeout for stopping an old replica when the service has no `stop_grace_period` (default: 10s)  |
| `--keep-failed`      | Leave a new replica that did not become healthy stopped instead of deleting it                  |
| `--dry-run`          | Check the services and print what a rollout would do; creates and stops nothing                 |
| `--external-healthd` | Use healthd but do not look it up, as for `up`                                                  |
| `--no-dns`           | Don't point the new replicas at the project's ic-dns, as for `up`                               |

Every option has an `INCUS_COMPOSE_ROLLOUT_*` environment variable
(`INCUS_COMPOSE_ROLLOUT_DNS_TTL` and so on); `--external-healthd` and `--no-dns`
read the same variables as `up`.

`--dry-run` goes through every check a rollout makes before it touches a replica
(the refusals below, ic-healthd running, the replicas present) and prints one
line per service with the replicas found, the creates and the timeouts it would
use. It writes nothing to Incus: it pulls no image (a pull may delete the copy
it replaces, and takes locks in a project of its own), neither builds one nor
copies one from the cache, creates no project (the image cache's included),
and does not add the project config keys a compose file has gained (`x-incus`
at the top level). The plan reads the image as the project has it; one that is
not there is named on a `not checked:` line, with the healthcheck it may
declare. A compose file that turns the healthcheck off (`disable: true` or a
test of `NONE`) is refused even with the image not there. What
it could not check for that reason follows the plan, on lines that read `not
checked:`, and the keys a rollout would add on a line of their own.

`rollout` acts on exactly the named services. It never follows `depends_on` and
never recreates or restarts a dependent: a service that depends on the one being
rolled out keeps the instance it has. A dependency the service declares with
`condition: service_healthy` is read before each create, neither started nor
waited for: when it is not running and `healthy`, `rollout` creates nothing and
exits with an error naming it. It does not touch the ic-healthd or ic-dns
sidecars either, where `up` would update them. A network or volume the compose
file has gained since the last `up` is created for the new replica, as `up`
would; existing ones are only read.

## What it does

For each replica, in turn:

1. create a new replica from the compose file as it is now (with `--pull`, from
   the image the registry holds now) and start it;
2. wait for ic-healthd to report it `healthy`, printing what the healthcheck
   says as it changes (`starting`, then `healthy`);
3. read every replica again and pick an old one to retire: one that is stopped,
   drained or not `healthy` before a healthy one;
4. set `user.label.dns.drain=true` on it, which takes it out of the service
   name in ic-dns, and wait `--dns-ttl` plus `--drain-delay` for clients to move
   off it;
5. stop it within the service's `stop_grace_period` (or `--stop-timeout`), and
   delete it.

Right before the drain, and again right before the stop, every replica is read
once more, in one round: the old one goes only while the declared count serves
without it (running, not drained, `healthy` as read then). If not, `rollout`
puts it back into the service name, takes away the new replicas of the run
that no longer serve (a rollback), and exits with an error saying how many
serve. A round whose reads take more than 10 seconds in all is not trusted,
since a verdict read at its start may have changed by its end; one that fails
or is that slow is made again, twice at most, after 2 and 4 seconds, and only
then does the run stop with an error, retiring nothing. During the grace the
other replicas are read every 5 seconds, with the same rules: the first round
read with fewer than the declared count serving puts the old replica back at
once, takes away the new ones that no longer serve, and fails the run; a round
that cannot be read lets the grace go on, but authorizes nothing, as the check
right before the stop still has to pass. That narrows the window in which the
service may have fewer healthy replicas than declared to about one step plus
the DNS TTL; it cannot close it, since ic-healthd may change a verdict at any
time and clients hold a record for its TTL. A
new replica that no longer serves when the replicas are read (ic-healthd
restarted it, or it turned `unhealthy`) is old again: it is the one taken away,
the old ones serve on, and `rollout` exits with an error.

After the grace, before the last round of reads, the old replica's health
checking is stopped (`user.healthcheck.stopped=true`): it is drained already,
and it ends ic-healthd's execs on it, which a stop waits out. A stop goes by the
round of reads right before it: one that would be sent more than 15 seconds
after that round ended (checked inside the client, after the waits before the
stop request; a client suspended for longer is still caught), or a victim ic-healthd starts again after its
stop, even right after a graceful stop that went through, takes a new round
first, three times at most; no kill goes out on an old round. On the try that
follows such a round, a victim that reads running again after its graceful
stop (as when the stop returns on a passing `Error` read) is killed every 5
seconds while it reads running, up to 30, as one whose stop ran out, if the
round is still fresh; else a new round decides. Whenever no stop
went through, the third try included, the old replica is put back into the
service name and its health checking resumed (`user.healthcheck.stopped`
cleared), and what went wrong on each try is reported. An interrupt is seen up
to the stop request, and a replica read stopped up to its delete. Each replica is
checked to be the instance first read under its name (`volatile.uuid`) before
it is stopped, deleted (again right before the delete, after the stop) or
taken away: one whose name
another instance took meanwhile is left alone, and the run fails. A replica
read stopped is deleted only while it is stopped, with no drain; one that
started again in between is left, and the run fails. With fewer than the
declared count serving without it, a stopped old replica goes only when a
replica of this run serves in the same round of reads (which repairs a service
that is all down); else it is kept, with ic-healthd's restart policy, put back
into the service name if a rollout drained it, the new replicas of the run
that do not serve are taken away, and the run fails, so a run again creates
afresh. No delete or take-down goes out without the instance's uuid known and
read again right before it; a replica gone meanwhile is not deleted by name.
An operator recreating the same name in the window left is not caught. A drain set by hand
(without the rollout's mark, below) is never taken off: such a replica does not
count as serving, and is replaced like any old one. One put back into the
service name by something else during its grace is not stopped.

A new replica that reports `unhealthy`, stops, fails to be created or started,
or is not `healthy` within the health timeout is deleted (or, with
`--keep-failed`, left stopped), and `rollout` exits with an error naming it.
Its health checking is stopped right at the verdict, so ic-healthd does not
restart it in the meantime; a delete that finds a replica busy with an
operation is made again after it, three times at most. A stopped replica that
carries a run's mark, a new one an earlier run left failed, is taken away
first, as failed new ones are, and is not kept as an old one.
One whose health cannot be read three times in a row is left as it is, since it
may be the one that serves, and `rollout` exits with an error. The old replica
was never drained, so it serves throughout.

Two `up` or `rollout` run at once on the same service are not supported: the
checks above catch a replica taken away or replaced under a run, they do not
coordinate two runs.

### The marks of a run

A run marks what it does on the instances' config, which is how a run picked up
again tells new from old:

- `user.incus-compose.rollout.id`: the run's id, on every replica it creates;
- `user.incus-compose.rollout.drain`: the run's id, next to
  `user.label.dns.drain`, on a replica it drains;
- `user.incus-compose.rollout.pre-start`: the run's id, on the replica that ran
  the service-wide `pre_start` hooks, written once they succeed and before it
  starts.

Once the rollout is over the `id` and `pre-start` marks are taken off the
replicas it leaves; the drain mark goes with the replica it is on. A service
with service-wide `pre_start` hooks keeps the replicas a run picked up again
made only once one of them records the hooks ran; else they are replaced, so
the hooks run. The marks, and the service and project labels, cannot be set
through `x-incus`: incus-compose sets the labels after it and drops the marks.

At its end a rollout reads its replicas once more, and fails when one it made
or kept no longer serves. A new replica that fails while it is the one that
serves (the old ones stopped meanwhile) is left as it is, not taken away. Each
read of a replica is bounded at 30 seconds. The drain label is read as ic-dns
reads it, trimmed: `" true "` drains.

Services named together are checked first, all of them, and rolled out one after
the other; the first failure stops the run.

### Names

The service keeps its name; its replicas need not keep theirs. A service is the
set of instances carrying its label, whatever their indexes, and that is what
`up` reconciles too (see [Replica names](#replica-names)). So a new replica
takes the lowest index no replica holds, and the old one it replaces leaves its
index free for the next:

- one replica: create `web-2`, retire `web-1`; the next rollout creates `web-1`
  and retires `web-2`;
- two replicas made in that order: create `web-3`, retire `web-1`, create
  `web-1`, retire `web-2`, leaving `web-1` and `web-3`.

Of the old replicas, the one to go first is a stopped, drained or unhealthy
one, then one made from an older config or image, then the oldest by its
creation, then the lowest index: with `web-1` made after `web-2` (by an earlier
rollout), `web-2` goes first.

That is one create per replica, and an old replica goes only while the declared
count of healthy replicas serves without it, as read right before (see above
for the window that leaves). Incus cannot rename a running instance, so the
names are not put back afterwards: `up` leaves the replicas where they are.

A new replica is placed where `up --scale <service>=N+1` would place a replica,
on a member none of the others runs on when there is one, and takes an index
no instance holds, the service's or not. The first replica a rollout creates
runs the service's `pre_start` hooks that are not `per_replica`, once per
rollout (see [The marks of a run](#the-marks-of-a-run)). A replica a profile
makes drained is taken away before it starts, and the run fails.

### Timeouts

The health timeout is `--health-timeout` when given, otherwise the longest
ic-healthd can take to reach a verdict, plus 30 seconds:

```
start_period + retries * (interval + timeout) + 30s
```

with docker's defaults for whatever is unset (`start_period` 0, `interval` and
`timeout` 30s, `retries` 3), so 3m30s for a healthcheck that sets nothing. The
healthcheck is the one the instance will run: the service's own, or its image's
`HEALTHCHECK`. A slow application should declare a `start_period` that covers
its start-up, which is what keeps `unhealthy` from arriving early.

### Interrupting

Ctrl-C (or SIGTERM) winds the rollout down and leaves a consistent state; a
second one kills it outright.

- While a new replica is being created, the create is let finish (Incus would
  carry on without us) and the replica is then deleted.
- While it starts or is checked, it is deleted. The old replicas were not
  touched.
- While an old replica is drained, up to the moment its stop is sent, it is put
  back into the service name, both replicas are left running, and `rollout`
  exits with an error. One drained by hand stays drained.
- Once an old replica's stop is sent, the stop and the delete are seen through.

### Running it again

A rollout that stopped, for whatever reason, can be run again and picks up from
what is there. Every replica found counts as one to replace. If there is one
replica more than declared (an interrupt while draining), one is given back
first: a stopped one or one that is not healthy before a healthy one, so the
service keeps N replicas serving. A rollout refuses to start with more than N+1.

A run that finds replicas marked by an earlier one, at least one of them still
what a create makes now (the service's config hash and image), picks that run
up: it takes its id, keeps the replicas marked with it once they are `healthy`,
and does not run the service-wide `pre_start` hooks again when a replica records
they ran. Otherwise it is a new run: every replica found counts as old, and the
hooks run with its first create.

A running replica left drained by an earlier rollout (its drain carries the
mark) stays drained and goes first when the others serve the declared count
without it; otherwise it is put back into the service name first. `up` does not
clear the drain label.

A marked replica that does not get to `healthy` is old again: when the drained
old replica is then the only one that serves, it is put back and the new one
taken away, a rollback, and `rollout` exits with an error. Health that cannot
be read stops the run before anything is retired.

With no mark at all, as `up` or a rollout from before the marks leaves them,
every replica up to date and more of them than declared, the newest ones that
serve are kept, up to the declared count, and the oldest are the surplus that
goes, with no create. With no mark and as many as declared, nothing tells the
new from the old, and a rollout of a service that did not change replaces them
all, as it is asked to.

When old replicas are left, the last line says to run `rollout` again. `up` is
safe on what a rollout leaves: it creates and deletes nothing while the count is
the declared one, and with one replica more it removes one, in the order
described under [Replica names](#replica-names), never the only healthy one. It
does recreate, stop-first, the old replicas whose compose definition has changed
since they were made, as it always does.

### Replica names

`up` reconciles a service on the replicas it has, not on the names `1` to `N`:

- as many as declared, whatever their indexes (`web-2` alone, or `web-1` and
  `web-3`): nothing is created or deleted;
- fewer: new ones take the lowest free indexes;
- more: the surplus goes, first the ones not running, then the drained ones,
  then the ones made from an older compose definition, then the ones with no
  config hash, then the ones made from another image than the one the service's
  image points at (when `up` has pulled it), then the highest index; but the
  ones kept hold as many healthy replicas as there are, up to the declared
  count. A replica with a healthcheck that has not reported `healthy` yet does
  not count as healthy.

A replica is one of the service's when it carries its
`user.label.incus-compose.service` label (and the compose project's) and is
named `<service>-<index>`. An instance named so without those labels, made by
hand or by another service, is never adopted: neither `up` nor `rollout`
counts, deletes or reuses it, and a new replica takes the next free index. A
replica's drain label and healthcheck are read from its expanded config,
profiles included, as ic-dns reads them. `exec`, `cp` and `port --index N` pick `<service>-N`;
without `--index`, the lowest that exists.

An older incus-compose (up to 1.4.0) reconciles by index: on a service left at
`web-2` with one replica declared, its `up` creates `web-1` and deletes `web-2`.

## Requirements

`rollout` talks to the rest of the system only through instance config, and
needs both of these:

- **ic-healthd** watching the project, which writes `user.healthcheck.status`.
  `rollout` refuses to run without it. A build that writes `starting` when an
  instance starts makes the progress readable; `rollout` only waits for
  `healthy` either way.
- **ic-dns** that honours `user.label.dns.drain` (a drained replica leaves the
  service name `<service>.<zone>` and keeps its own name) and answers a service
  name only with the replicas whose healthcheck reports `healthy`, falling back
  to all of them when none does. That is what keeps a new replica out of the
  name until it is ready, and the old one out of it before it stops.

With an ic-dns that has neither, the rollout still keeps a healthy replica in
place at every step, but clients can reach the new replica before it is ready
and the old one until it stops. Nothing detects which ic-dns runs.

The service names incus-compose writes into dnsmasq on a bridge network without
ic-dns are not maintained by `rollout`: they would list the new replica before
it is ready. Run `up` afterwards on such a project.

## What it refuses

`rollout` runs the new replica next to the old one, so it refuses a service
where two of them cannot run at once, and says why. `restart` is the way to
update those:

- no healthcheck (from the service, its image, or a profile the compose file
  gives the service now, read from Incus): there is nothing to wait for,
  and ic-dns serves a replica nothing checks as soon as it has an address;
- `container_name`: one fixed name;
- published `ports` or a `proxy` device: one host address;
- a fixed IP or MAC address (`ipv4_address`, `ipv6_address`, `mac_address`, or
  `ipv4.address`, `ipv6.address`, `hwaddr` on a NIC through `x-incus` or a
  profile);
- DNS aliases (`networks.<net>.aliases` or `dns.aliases`): only the first
  replica carries them, so they would go unanswered while it is replaced;
- a `dns.drain` label (or `user.label.dns.drain` through `x-incus`) set to
  true: every new replica would be born out of the service name, and could
  pass its check while nothing serves under the name;
- on a cluster, a custom volume on any pool but `cephfs`: a block-backed volume
  (`ceph`, `linstor`, `lvmcluster`, ...) is mounted on one member at a time, and
  the new replica may land on another. On a standalone server both replicas
  mount it from the same host. The paths an image declares as volumes become
  volumes too; `x-incus-compose.auto-volumes: false` leaves them to the image. A
  volume that is accepted is mounted by both replicas at once: a service that
  must be its data's only writer (a database) is to be marked as below;
- `deploy.mode: global`: one replica per cluster member;
- `deploy.replicas: 0`, or a service with no replica yet (create it with `up`);
- a service marked `x-incus-compose.rollout: false` (below).

Devices are checked both on a replica as Incus has it (with what profiles and
the image added) and as the compose file declares them now.

### Services that must not run twice

During a rollout the old and the new replica run side by side for as long as the
new one takes to become healthy and the old one to drain. A service that must
exist once - a scheduler such as celery beat, including one running inside a
container that bundles several processes - would run twice in that window, and
schedule its work twice. Mark it so that `rollout` refuses it:

```yaml
services:
  app:
    image: registry.example.com/app:latest
    x-incus-compose:
      rollout: false
```

and update it with `restart` (or `up --recreate --no-deps app`) instead.

## Compared with up

`up --pull always` and `up` with a changed compose file recreate outdated
replicas stop-first, in batches of `deploy.update_config.parallelism`;
`deploy.update_config.order: start-first` is not supported there. `rollout` is
the start-first variant, for one service at a time, and leaves the rest of the
project, its sidecars and its networks alone.

_Since: Unreleased_
