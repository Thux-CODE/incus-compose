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
use. It still pulls an image that is missing from the project, since the
healthcheck an image declares is part of the check.

`rollout` acts on exactly the named services. It never follows `depends_on` and
never recreates or restarts a dependent: a service that depends on the one being
rolled out keeps the instance it has. It does not touch the ic-healthd or ic-dns
sidecars either, where `up` would update them. A network or volume the compose
file has gained since the last `up` is created for the new replica, as `up`
would; existing ones are only read.

## What it does

For each replica, in turn:

1. create a new replica from the compose file as it is now (with `--pull`, from
   the image the registry holds now) and start it;
2. wait for ic-healthd to report it `healthy`, printing what the healthcheck
   says as it changes (`starting`, then `healthy`);
3. set `user.label.dns.drain=true` on an old replica, which takes it out of the
   service name in ic-dns, and wait `--dns-ttl` plus `--drain-delay` for clients
   to move off it;
4. stop it within the service's `stop_grace_period` (or `--stop-timeout`), and
   delete it.

A new replica that reports `unhealthy`, stops, fails to be created or started,
or is not `healthy` within the health timeout is deleted (or, with
`--keep-failed`, left stopped), and `rollout` exits with an error naming it. The
old replica was never drained, so it serves throughout.

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
- two replicas: create `web-3`, retire `web-1`, create `web-1`, retire `web-2`,
  leaving `web-1` and `web-3`.

That is one create per replica, and at every moment the service has at least N
healthy replicas serving. Incus cannot rename a running instance, so the names
are not put back afterwards: `up` leaves the replicas where they are.

A new replica is placed where `up --scale <service>=N+1` would place a replica,
on a member none of the others runs on when there is one. The first replica a
rollout creates runs the service's `pre_start` hooks that are not `per_replica`,
once per rollout.

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
- While an old replica is drained, it is put back into the service name and both
  replicas are left running.
- Once an old replica is being stopped, the stop and the delete are seen
  through.

### Running it again

A rollout that stopped, for whatever reason, can be run again and picks up from
what is there. Every replica found counts as one to replace. If there is one
replica more than declared (an interrupt while draining), one is given back
first: a stopped one or one that is not healthy before a healthy one, so the
service keeps N replicas serving. A rollout refuses to start with more than N+1.

A running replica left drained by a rollout that was killed is put back into the
service name first: only a rollout drains, right before a stop. `up` does not
clear the drain label.

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
  then the ones made from an older compose definition, then the highest index;
  but a healthy replica stays when there is one.

A replica is one of the service's when it carries its
`user.label.incus-compose.service` label (and the compose project's) and is
named `<service>-<index>`. `exec`, `cp` and `port --index N` pick `<service>-N`;
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

- no healthcheck (from the service or its image): there is nothing to wait for,
  and ic-dns serves a replica nothing checks as soon as it has an address;
- `container_name`: one fixed name;
- published `ports` or a `proxy` device: one host address;
- a fixed IP or MAC address (`ipv4_address`, `ipv6_address`, `mac_address`, or
  `ipv4.address`, `ipv6.address`, `hwaddr` on a NIC through `x-incus` or a
  profile);
- DNS aliases (`networks.<net>.aliases` or `dns.aliases`): only the first
  replica carries them, so they would go unanswered while it is replaced;
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
