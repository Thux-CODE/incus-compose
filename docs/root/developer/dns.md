---
date: 2026-08-28T01:33:50Z
dateCreated: 2026-08-14T11:46:35Z
leafwiki_id: BMZeYtUDg
leafwiki_title: DNS Architecture & Daemon Internals (ic-dns)
leafwiki_created_at: "2026-08-14T11:46:35Z"
leafwiki_updated_at: "2026-08-28T01:33:50Z"
leafwiki_creator_id: system
leafwiki_last_author_id: system
---

# DNS Architecture & Daemon Internals (ic-dns)

The DNS half of the event chain: it folds Incus lifecycle events into DNS
records and answers queries from them. User-facing configuration and compose
usage is documented on the main [DNS](/dns) page; this document covers how
`ic-dns` is built, its plugin chain, and how to run the standalone daemon
directly.

## A Plugin Owns What It Serves

It is not a plugin that hands records to a server somewhere else. It holds the
engine, starts the listener, and puts the forwarder after itself, so `main`
composes a chain and knows nothing about DNS - which is what lets a second
binary be a copy of `main` with a different list.

Its listener outlives its fold loop. Answering from the last snapshot while the
chain drains beats refusing, and the two cannot hurt each other: one goroutine
writes an `atomic.Pointer` and every query goroutine reads it. That is the only
crossing in the package, and it is why there is no lock in it.

It serves UDP and TCP together, always: a truncated UDP reply is only useful to
a client that can retry over TCP. A panic from the query chain after the engine
is recovered and answered SERVFAIL - the one place in this package that recovers
at all, unlike the rest of the ievent chain, because `Chain` is arbitrary
CoreDNS plugins this package did not write and cannot vouch for. The adapter
answering these is hand-shaped after CoreDNS's own `dnsserver.Server.ServeDNS`,
less what a single-chain binary has no use for - server blocks, views, proxy
protocol - and has to be diffed by hand against upstream on every CoreDNS bump.

## Two Halves, One Atomic Pointer

```
events -> fold -> build -> Snapshot -> atomic store
                                          |
query -----------------------------> atomic load
```

**Work happens when things change, not when queries arrive.** A whole-fleet
snapshot is built when something moves, records rendered and filtered ahead of
time, and answering is then three map lookups - no filtering, no intersection,
no I/O. A query never touches Incus.

## The Engine Derives Nothing

`ecs_view`, under here, holds the records and answers. It knows nothing about
Incus, and `just boundary` enforces that it cannot even see where a record came
from. What keeps the query path free of derivation is that it _cannot_ derive.

A view is a set of network keys. The answer to a query depends on the querier
only through that set, so every querier on the same set gets identical answers
and they share one view.

## Placing The Querier

A querier is identified two ways: an EDNS0 client-subnet option, if a resolver
relayed one, otherwise the query's own source address - the option wins, since a
resolver forwarding on a client's behalf puts that client's address there for
every query it relays.

That address is checked against an instance's own claim first, and only then
against which known subnets it falls inside. Belonging to no host but sitting
inside one or more known networks makes it an anonymous member of all of them,
which is what lets something elsewhere on a bridge resolve records for the other
hosts on that bridge without being one itself.

An address that cannot be placed on any network at all is refused outright,
never answered from everything: there is no default view, because guessing would
leak one network's records to a querier on another, and a real client is always
on some network this server knows about. The zone apex is the one answer that
does not vary by querier - its scope is left at 0 so a resolver caching an
echoed subnet reply may share it fleet-wide - everything else is scoped to the
querier's whole address, since a different address may be a different view.

## Building Is Fleet-Wide, Never Per Project

A set of networks can span projects: two instances on one shared bridge sit in
different projects and different zones while being on the same wire. Building
per project could only ever fill a view with the names of whichever project
built it, so those two would go mutually blind - and both halves of that read as
NXDOMAIN, which is indistinguishable from correct isolation.

## Labels Layer, Project Under Instance

A project's own `dns.*` labels are the defaults its instances start from; an
instance's own labels override them field by field. `ns` and `transfer` are the
exceptions: a name server and the decision to hand a zone over are the zone's,
so both are read off the project alone and dropped if an instance sets them. Two
projects resolving to one zone name union, so either one's `transfer` opts it
in. `aliases` is the reverse exception - it never inherits, since a project-wide
alias would claim one name for every instance in it.

The zone itself is `<project>.<suffix>` unless `user.label.dns.zone` names one
instead. The name a scaled service answers to is `user.label.dns.service`, or
`incus-compose.service` when compose set it - compose wins, so a fleet built
from a compose file is named by the file that owns it and not by a label
somebody typed once by hand.

## Forward Names

Held by project **and** name, because two projects may each have a `web` and
they are different hosts in different zones.

- `<instance>.<zone>` always
- `<service>.<zone>` when labelled - a scaled service's replicas append to one
  slice, which is all "a service name resolves to every replica" means
- aliases, as CNAMEs

An address claimed by more than one instance resolves to `AmbiguousView` rather
than to either of them. Two projects on overlapping subnets really can hand out
the same one, and deciding by map order would answer a querier with the other's
view, non-deterministically.

## Aliases And CNAMEs

`user.label.dns.aliases` becomes a CNAME onto `<instance>.<zone>` rather than a
second copy of the addresses, so a host that moves changes them in one place.

Rendered per network key from the target's own rendered records, which is what
makes an alias reach exactly as far as the name it points at and never wider.
The chase is done at build time - the CNAME goes into the A and AAAA sets as
well as its own, canonical name first - so answering through one stays three map
lookups.

One CNAME value serves every network the name is reachable on, since it is the
one record that does not vary by network. It is written into each of those
views, and a querier resolves to exactly one of them, so nothing has to
de-duplicate it at query time.

An alias is dropped, never resolved by picking, when it collides: a name a host
or service already answers to, a name two instances both claim, or a zone apex.
Each would put a CNAME beside other records or two CNAMEs at one name.

An absolute alias landing outside every zone gets one invented for its parent,
marked `Fallthrough`: that zone claims the aliased names and hands everything
else to the forwarder, so one label on one instance cannot blackhole a domain
the fleet still resolves. A name it _does_ hold is answered under the ordinary
rule, invisible included, so falling through cannot be used to tell a hidden
name from an absent one.

## Reverse Records

Every address is filed backwards under the name `dns.ReverseAddr` gives it,
keyed by the same network the forward record is keyed by. That keying is the
whole of it: a PTR is stored per view exactly as its forward name is, so an
address on a network the querier is not on reads as NXDOMAIN exactly as its
forward name does.

A reverse zone is the `/24` or `/64` an address lands in, taken from **the
address rather than from its network's prefix**. A subnet Incus manages but
nothing sits on is therefore never claimed. Where a bridge is the usual `/24` or
`/64` the two derivations agree; where it is a `/22`, deriving from the prefix
would make this server authoritative for the enclosing `/16`.

The network's prefixes still decide _whether_ there is a reverse at all: an
address outside every prefix of the network it sits on is served forward and not
backward. One instance bridged onto a public `/24` would otherwise have this
server answering NXDOMAIN for the rest of it. It is the one place the two
directions deliberately disagree.

## NS Records

`Zone.NS` is the apex NS set, read straight off `user.label.dns.ns` and resolved
against the zone the way an alias is - a trailing dot absolute, anything else
relative. Empty falls back to a placeholder name nothing answers for, harmless
until the zone is transferred or delegated.

It is not derived from the fleet. A secondary that would receive it is outside
the fleet, or it would not need a transfer, so there is nothing in `held` to
read it off - the operator's own list, typed once at the delegation's parent, is
the only source there is.

Two projects resolving to one zone name union their `ns` labels into one set,
the same way their instances union into one zone. A reverse zone unions every
contributing project's `ns`, since it belongs to no project of its own.

A zone `aliasZones` invents for one absolute alias never gets one: it is
`Fallthrough`, so `Answer` never reaches the apex branch for it, and handing a
name server for a domain this server holds one name in would make it
authoritative for the rest of that domain too.

## Zone Serials

A zone's serial moves when what a transfer of that zone carries moves, and at
no other time. **Each serial is kept with a digest** (`zoneDigest`) of the zone
it was published with - its NS set and every record, every view folded
together, the SOA left out. A patch marks the zones it touched, and publish
compares each marked zone's digest with the one its serial holds: different
steps the serial, equal steps nothing, so a view moving or a read confirming
what was there claims nothing.

Only once a round has ended in this run (`state.complete`), whatever the stamp
of the event that changed it: before it a zone holds whatever the replay and
the reads have reached, and a step is a claim that nothing has confirmed. The
digest is left as it was stored until then, so the first publish once complete
compares every zone against what its serial was published with and steps each
that differs, once. That is what catches what the replay gets wrong: a delete
inside its window, a peering that changed which service replicas answer, an
enricher store - written on its own clock - newer than this one's. The digest
goes to the cold store with the serial. Its first publish is a birth rather
than a step, since a secondary reads zero as "no zone here"; a zone born before
the round takes what it holds at the round as what 1 describes, since nothing
could transfer it before.

A zone nothing is left in stops being served but keeps its serial for good,
under a digest no content has: forgetting it would publish it at 1 when its
names come back, which a secondary that polled before may hold for the old
content. A file from before the digest is taken on trust as it was then - the
replay's first publish of a zone is what its serial is taken to describe -
unless it says a step was owed.

The plugin stamps it because it is the only thing that knows what it published
last - the engine is a read-only view and holds nothing to compare against.

## Zone Transfer

AXFR and IXFR are answered by `xfr`, a position of its own ahead of the engine
in the query chain, from a snapshot **published to it separately**. The engine's
pointer is not read through: a transfer is the one answer not filtered by who is
asking, so it derives what the query path is built not to, and one
`atomic.Pointer` with one writer is cheaper than a second reader of somebody
else's. `publish` writes the same finished value twice.

Both answer from that snapshot rather than a journal - there is no delta to cut,
so an IXFR asking for anything but the serial already served gets the whole zone
anyway. UDP is refused outright; a transfer is a stream of messages and UDP
carries one.

Nothing goes over before the first round has ended (`xfr.SetComplete`, after
the publish that ends it): until then a zone holds what the replay has reached
under the serial a previous run published whole, and a secondary taking half of
it would never see that serial move for the rest. A transfer then is `SERVFAIL`,
so the secondary keeps what it holds and tries again; the flag is loaded before
the snapshot, so a transfer reading it set answers from the round's publish or a
later one.

Two gates stand before the snapshot is even read: the peer's address against
`--allow-transfer` (no prefixes allows nobody, so transfer is opt-in at both the
zone's `transfer` label and the listener's config), and - once secrets are
configured - the request's TSIG status, which becomes the only assertion of who
the peer is once it exists. The request must name an apex: `ZoneOf` takes the
longest, so a name inside a zone resolves to the zone holding it and is refused
rather than served. A zone served without `Transfer`, and a `Shadowing` zone
invented for one absolute alias, are refused with no case of their own.

A zone goes over whole or not at all, which is the one answer here not filtered
by who is asking: every view a name holds is folded together rather than one
querier's, because the serial is per zone and not per view - two secondaries
handed different records under one serial could never tell that they had been.

**The name tree's keying is what enumerates a zone.** Names are keyed
root-first, so a zone is a byte-wise prefix of every name in it and one walk
finds them, in one deterministic order - two transfers of an unchanged zone are
the same byte stream without sorting anything. The same keying is why **every
name is checked back against `ZoneOf`**: a zone underneath this one is a prefix
match too, and carrying its names would hand a secondary a delegation nobody
made.

An alias shares its name with the records it was chased into, so a zone may not
hold both sets; on the wire the CNAME goes alone.

Everything is rendered and cut into envelopes - capped under the 64 KiB wire
limit, leaving room for the framing and a TSIG record - before the peer's
`Transfer.Out` reads any of it, off a channel that is already closed: nothing is
left running if the peer hangs up mid-stream, because there was never a producer
goroutine to hang.

## The Cold Store

With `ColdDir` set, every zone's serial is written to disk, with the digest of
what the zone carried under it. Records come back from the enricher's own store
or an Incus fleet read; a serial cannot be recovered from anywhere, and a
secondary reading one going backwards re-transfers on every restart of this
process. With `--peer-views` the peering goes with them - the last one that
counted as known: the one restored, or a read asked for after the last peer
event that covered every network a name was on. A read this run would not turn
warm on is not stored as one the next run would.

On a restart the enricher replays what it held before it has read anything, as
`instance-updated` events stamped cold and marked `Restored`, so names are
answered from the first publish - one `publishWindow` after the start - rather
than falling through to the forwarders until the first round ends. Nothing is
`Ready` and every answer is clamped until that round does, and no serial moves
while cold: a zone comes back under the serial it was stored with. A zone whose
names have not come back by a publish stops being served but keeps its serial,
then and once warm - the replay may simply not have reached it yet, and
forgetting it would publish it at 1.

A read in this run that finds what was restored changes no record and moves no
serial, but replaces the event held for that instance, so what is held stops
reading as restored. What the round finds changed while the process was down is
published at once under the serial the zone was stored with, and stepped once
by the first compare after the round (see Zone Serials), which is when a
secondary transfers it.

How long that takes is how long the first round takes. One that breaks - Incus
refusing the listings at start, say - leaves the restored names served, clamped
and not `Ready`, until a round reaches its end; how soon one is tried again is
the enricher's (see the sweep retry in [[developer/ievent/enricher|enricher]]).

Encoding happens on the fold goroutine and writing on its own, so a slow disk
cannot stall the chain. What crosses between them is a finished `[]byte`, which
is why the writer _cannot_ reach into live state rather than merely not doing
so. One slot, and a queued encoding is discarded rather than waited on: a newer
one says everything it said.

The on-disk form is explicit types with explicit tags, because the file outlives
the process that wrote it - a field renamed in Go must not silently change what
a running deployment can read back. A file of any other version is ignored
rather than migrated, since this is a cache and starting without it costs one
whole-fleet read.

## The Chain Reconciles What Is Served

`dns` never reads Incus directly. It discards the connection at `Setup` and
learns about the fleet entirely through the chain: instances, networks, and
labels arrive as enriched events, and disappeared instances arrive as synthetic
`instance-deleted` events emitted by the enricher's round prune.

An event arriving on a full inbox is dropped, and marked so - but for a
restored one, which spills instead: the enricher replays its whole store at
once before anything reads Incus, and a stored name dropped there stayed
`SERVFAIL` until the round read it. What arrives while something has spilled
goes behind it, so nothing overtakes the replay; the live events among it are
bounded by the inbox size, and dropped past it as before. The fold takes what
is left on the inbox and the spill under the lock `Handle` takes, as one cut,
so nothing that arrives meanwhile lands between the two.

Patches collect in a 150ms `publishWindow` before a snapshot is published, so an
instance that moved and moved back inside one window compares equal and reaches
the trees not at all. On `enricher/sweep-end`, any pending window is flushed and
published immediately.

## Warm Is Not This Plugin's To Set

The enricher raises `ChainState: ChainWarm` on `enricher/sweep-end`, so warm has
already been stamped by the time that event reaches here. `dns` reads it to
tell an ordered signal from a leftover (below); whether a serial may move is
its own `state.complete`, set by the round.

## A Cold Server Fails Its Misses

Between a start and the first `enricher/sweep-end`, what is published is only
what has been read so far: a project not reached yet has no zone, and a zone
being filled lacks the instances still to come. Answering from that as if it
were whole hands out wrong answers that look final - a name under `--suffix`
with no zone fell through to `--forward`, whose `NXDOMAIN` the client believes,
may cache, and does not retry on its next `nameserver`.

So until then every miss is `SERVFAIL` with the extended error `Not Ready`:
inside a zone (missing, invisible, or a querier not placed yet - its own
instance may be the one still unread), and anywhere under `--suffix`, which
`dns` hands the engine as `ECSView.Claims` (an empty or root suffix claims
nothing). Hits, `NODATA` and the apex answer as usual, clamped to 5 s, unless
`--cold-answers` is off (below), and names outside both fall through, so the
internet keeps resolving.

A name under `--suffix` in no zone known so far is not failed outright: names
there that no project builds - the cluster members', which the uplink's dnsmasq
resolves - would otherwise fail on every restart, and a client checking an
instance through one of them (an `ic-healthd` talking to its Incus node) would
see the instance go down with it. The engine hands it to `Next`, the query chain
a warm miss falls through to, through a
[`nonwriter`](https://pkg.go.dev/github.com/coredns/coredns/plugin/pkg/nonwriter)
that catches the reply. `NOERROR` goes out as the forwarder wrote it, not
authoritative and not clamped, with records or without: `NODATA` says upstream
holds the name, so no project builds it, and the AAAA of an IPv4-only node is
exactly that - failing it would fail the node's whole lookup on musl, which
sends A and AAAA together and gives up on either's `SERVFAIL`. A truncated
`NOERROR` goes out as it came too, so the client retries over TCP and the TCP
reply is judged on its own. Anything else - `NXDOMAIN`, another rcode, an error,
nothing written - is what an unread project looks like from upstream too, so it
is the cold `SERVFAIL`, truncated or not: a client that does not ask again over
TCP takes a truncated `NXDOMAIN` as final. A forwarder that does not answer holds the
query for `forward`'s own 5 s first, as it would warm. The cache sits in front
of `forward`, so what it stores here is what upstream said and changes nothing
once warm: a project's zone answers before the cache is reached.

glibc, musl and Go move on to the next listed server on `SERVFAIL`, which is
what lets two `ic-dns` in `dns.nameservers` ride out a rolling restart. With a
single server the cost is a temporary failure instead of a wrong answer, and on
musl each such miss blocks for its 5 s timeout first. The measurements are in
[ecs_view's README](https://github.com/lxc/incus-compose/blob/main/cmd/ic-dns/dns/ecs_view/README.md#before-the-first-read);
systemd-resolved has not been measured.

The claim is the whole suffix. A deployment that forwards part of it to another
`ic-dns` - two serving different projects under one suffix - gets that one's
`NOERROR` answers, `NODATA` included, passed on while this one is cold, and
`SERVFAIL` where it says `NXDOMAIN`.

### Hits Before Warm: `--cold-answers`

What a cold server answers is what has been read so far and, where the enricher
replays what it held before the restart, records from then, which can be out of
date: an address changed while the server was down is served as a final
`NOERROR` until the round reads it again. `--cold-answers`
(`INCUS_COMPOSE_DNS_COLD_ANSWERS`, default `true`) decides what to do about it:

- **on** (the default): hits, `NODATA` and the apex are answered, TTL clamped to
  5 s. With a single server a name that may be a few seconds old beats no name
  at all.
- **off**: every query inside a zone of ours is `SERVFAIL` until warm, in the
  same shape as a cold miss - extended error `Not Ready`, `AA` unset, counted as
  `servfail`, reverse zones and a `--trusted-client` included: the router's
  answer from every view is as old as anybody's. There is no per-record
  provenance: a name read since the start fails along with one carried over from
  before it.

Turn it off only when every client lists a second `ic-dns`: glibc, musl and Go
then ask the other one instead of taking a stale record. With a single server it
turns every internal lookup into a temporary failure until warm. Names outside
the zones are forwarded either way, and once warm it changes nothing.

### What Warm Means

Warm is the end of a round, and the enricher ends a round only once it has read
what it listed: a project whose instance listing failed makes the round fail
and be tried again, and an instance listed that nothing holds holds the end
until a read of it lands, it goes, or a minute has passed. Past that minute the
view turns warm with that hole, and the name is `NXDOMAIN` until a read lands.

A round that fails outright - `GetProjectNames` timing out, say - raises no
sweep-end, and is tried again after a backoff of 1 s doubling up to 30 s: the
view stays cold, every miss `SERVFAIL` but those the forwarder answers
`NOERROR`, until one reaches its end.

The query path loads warm before the snapshot. The fold publishes the snapshot
that ends the round and only then turns warm, so a query reading warm answers
from that snapshot or a later one; the other way round it could take a snapshot
from before the last name and the warm after it.

### Turning Warm, In Order

Warm is the engine's own latch rather than `ChainState`. It sets once a round
has ended in this run and the peering is known (`settle`), and it never clears.
Healthy is set apart from it, on the same and a round having ended since the
stream was last lost: the peering can become known while the stream is down,
and the zones are whole then - answered clamped rather than failed as cold.
`ChainState` drops to cold on every lost stream, while the zones it leaves
behind are full - failing misses for a blip would fail them on every replica at
once, since an Incus outage drops every stream together. A lost stream clamps
the TTL and nothing more, as before.

`rounded` runs only on an ordered signal, because warming early turns names not
read yet into final `NXDOMAIN`s:

- a sweep-end that was not dropped. One dropped on the way - at a full enricher
  inbox, say - skipped the line and overtook the reads still in flight before
  it, so it rounds nothing;
- while no round has ended since the stream was last lost (`state.fresh`), any
  other event stamped `ChainWarm` that was not dropped: it walked the chain
  behind the sweep-end, so everything that round read has been folded. One
  stamped before the last lost stream is a debounced leftover and does not
  count.

A sweep-end that cannot be used - dropped at `dns`'s own full inbox, or arriving
dropped - is asked for again: `dns` raises `dns/round-lost`, which enters at the
head behind everything already in the chain and comes back stamped warm. Without
it a quiet fleet could leave the view cold until the next round. After a lost
stream the same holds for healthy: a dropped first sweep-end after the
reconnect is asked for again too, or the view would stay unhealthy, and
`/ready` down, until the next round.

Neither signal counts while the stream is down, and a reconnection takes back
what came while it was. Every event carries the stream session the source
minted it in (`Event.Session`), and the end of a round names the session of
the connected that started its run: the source takes it only within that
session with the stream open, so the end of a run read before the stream went,
or with no stream ever open, never reaches the chain. Whether the stream is up
is told by the sessions too, not by which control arrived last - one dropped
on a full inbox goes past the fold ahead of what is queued: a disconnected of
a session older than the one up is a leftover, and any event of a session newer
than the one that went says the stream is back, its connected lost or not. A
warm event counts for a round only in the session up now.

`/ready` follows the same signal, with the same rule. `http` turns ready on a
warm event only when it was not dropped and was stamped after the last lost
stream or reconnection, and it sits after `dns`, so the event that makes it ready is one `dns`
has already folded: `/ready` is never up while misses still fail. Once up, only
a lost stream takes it back - the moment `dns` marks the view unhealthy - and
it comes back with the next round, as healthy does. Nothing else does: an
event stamped `ChainCold` after the round, a read that failed or was dropped,
says when that event was minted, not what is served. A read sent before the
round ended can land after it still stamped cold, and clearing on it left
`/ready` at 503 until the next round, hours later. The answers stay warm
through a lost stream, deliberately: `/ready` down is "nothing is confirming
this", not "this is cold".

## Readiness Is An Event

`dns` raises `dns/ready` through `Command` when what it published becomes worth
answering from. It does not hold a reference to [[developer/ievent/http|http]],
and http holds none to it: the event goes in at the head and is folded in order
against the events that caused it.

That announcement is for whoever is watching the chain. What `/ready` itself
latches on is `ChainState`, which rides on the same sweep-end, and the stream's
own `source/disconnected` - the two edges `dns` turns healthy on and off - so
the two agree without either plugin knowing the other exists.

Edges rather than a level, so whoever folds them starts not-ready - the truth
before anything has been read.

## Running the daemon directly

`incus-compose up` creates the `ic-dns` sidecar and manages its lifecycle
automatically. You can also run the `ic-dns run` binary yourself — on the host
or inside a custom container — and configure it using the CLI flags or
environment variables below.

Every flag has a matching environment variable:

### Connecting To Incus

| Flag                             | Env                                             | Default              | Description                                                |
| -------------------------------- | ----------------------------------------------- | -------------------- | ---------------------------------------------------------- |
| `--incus`                        | `INCUS_COMPOSE_DNS_INCUS`                       |                      | URL of the Incus API                                       |
| `--token`                        | `INCUS_COMPOSE_DNS_TOKEN`                       |                      | One-time trust token                                       |
| `--data-dir`                     | `INCUS_COMPOSE_DNS_DATA_DIR`                    | `/var/lib/dns-incus` | Persistent directory for certificates and cold-start state |
| `--secrets-dir`                  | `INCUS_COMPOSE_DNS_SECRETS_DIR`                 | `/run/secrets`       | Directory holding trust tokens                             |
| `--client-cert` / `--client-key` | `INCUS_COMPOSE_DNS_CLIENT_CERT` / `_KEY`        |                      | Client certificate and key for authentication              |
| `--restricted`                   | `INCUS_COMPOSE_DNS_RESTRICTED`                  | `false`              | Enroll confined to `--project`                             |
| `--remote` / `--use-remote`      | `INCUS_REMOTE` / `INCUS_COMPOSE_DNS_USE_REMOTE` |                      | Connect using an existing remote from Incus CLI config     |

### Choosing What To Serve

| Flag               | Env                                | Default                       | Description                                 |
| ------------------ | ---------------------------------- | ----------------------------- | ------------------------------------------- |
| `--suffix`         | `INCUS_COMPOSE_DNS_SUFFIX`         | `incus`                       | TLD used for default project zones          |
| `--project`        | `INCUS_COMPOSE_DNS_PROJECTS`       |                               | Comma-separated list of projects to serve   |
| `--project-marker` | `INCUS_COMPOSE_DNS_PROJECT_MARKER` | `user.label.dns.scope=global` | Project label marker that opts a project in |

### Where To Listen

| Flag               | Env                                                       | Default | Description                                                   |
| ------------------ | --------------------------------------------------------- | ------- | ------------------------------------------------------------- |
| `--listen`         | `INCUS_COMPOSE_DNS_LISTEN`                                | `:53`   | DNS listen address (UDP and TCP)                              |
| `--allow-transfer` | `INCUS_COMPOSE_DNS_ALLOW_TRANSFER` / `DNS_ALLOW_TRANSFER` |         | CIDR(s) that may ask for a zone transfer; empty allows nobody |
| `--http`           | `INCUS_COMPOSE_DNS_HTTP`                                  | `:9153` | HTTP endpoint address (`/metrics`, `/health`, `/ready`)       |
| `--forward`        | `INCUS_COMPOSE_DNS_FORWARD`                               |         | Upstream DNS server(s) for unresolved external domains        |

### Tuning The Chain

| Flag                    | Env                                     | Default | Description                                               |
| ----------------------- | --------------------------------------- | ------- | --------------------------------------------------------- |
| `--ttl`                 | `INCUS_COMPOSE_DNS_TTL`                 | `5`     | TTL (in seconds) for served records (up to 3600)          |
| `--debounce-window`     | `INCUS_COMPOSE_DNS_DEBOUNCE_WINDOW`     | `250ms` | Quiet period before flushing burst updates                |
| `--workers`             | `INCUS_COMPOSE_DNS_WORKERS`             | `16`    | Parallel Incus API read workers                           |
| `--read-timeout`        | `INCUS_COMPOSE_DNS_READ_TIMEOUT`        | `10s`   | Timeout budget for single Incus reads                     |
| `--sweep-project-delay` | `INCUS_COMPOSE_DNS_SWEEP_PROJECT_DELAY` | `30s`   | Delay between consecutive project sweeps                  |
| `--sweep-read-delay`    | `INCUS_COMPOSE_DNS_SWEEP_READ_DELAY`    | `5s`    | Delay between instance reads in a sweep                   |
| `--echo-subnet`         | `INCUS_COMPOSE_DNS_ECHO_SUBNET`         | `false` | Echo back RFC 7871 client subnet                          |
| `--cold-answers`        | `INCUS_COMPOSE_DNS_COLD_ANSWERS`        | `true`  | Serve held names before warm; `false` makes them SERVFAIL |
| `--exclude`             | `INCUS_COMPOSE_DNS_EXCLUDE`             |         | Chain position(s) to leave out (`debounce`, `http`, etc.) |
| `--log`                 | `INCUS_COMPOSE_DNS_LOG`                 | `INFO`  | Log level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`)     |

`--ttl` is short on purpose. A fleet moves, and a resolver that cached an
address for an hour is one handing out an address that has been reassigned.

`--exclude` takes `debounce`, `http` and the log positions. The enricher and
`dns` may not go: without the enricher nothing is ever read, and the process
would start, answer, and serve nothing.

## HTTP Endpoints & Liveness

| Path       | Description                                                                    |
| ---------- | ------------------------------------------------------------------------------ |
| `/metrics` | Prometheus metrics for all CoreDNS and ievent plugins                          |
| `/health`  | Liveness check (stream is connected and has processed events recently)         |
| `/ready`   | Readiness check (a round of the fleet has ended and the event stream is alive) |

The two answer different questions on purpose. The only sensible response to
`/health` failing is a restart, and a restart does not fix an Incus that is down
— it throws away everything held and answers nothing until the fleet has been
re-read. So **a lost stream is unready, never unhealthy.**

A round does not make it unready, and neither does a read that failed. One is
always running and the server answers from what it published last throughout,
so `/ready` stays up and nothing pulls the process out of rotation for it. Once
up it comes down only with the event stream, and back with the next round.

The fleet is read a name at a time rather than all at once, so those two delays
are what decides how long a change nothing announced goes unnoticed: roughly
`--sweep-read-delay` times how many instances there are. There is no safe
direction: long leaves a wider window for a quirk; short pays for the round and
the event stream at the same time. The first round after a start or a reconnect
ignores both and runs flat out, because nothing is served until it lands.

## Known Gaps

- **Cold misses outside `--suffix` are claimed only once their zone exists.** A
  zone named by `user.label.dns.zone` outside the suffix, and a reverse zone,
  answer `SERVFAIL` for a miss before warm only after something has been read
  into them; before that the query falls through to `--forward`. The cold store
  holds every zone published last time, but not whether it was `Fallthrough`,
  and claiming the parent domain of an absolute alias would fail every other
  name in it.

- **Upstream metrics label off `metrics.WithServer`**, which reads a context key
  `core/dnsserver` sets - and nothing here runs it. So `coredns_cache_*` and
  friends carry `server=""`. Ours does not: `ecs_view` takes the name as a field
  at construction.
- **Any upstream plugin links `core/dnsserver`**, because each carries a
  `setup.go` in its own package and Go compiles a package whole. It is a toll
  paid once rather than per plugin - `forward` is 408 packages and `cache` 415,
  nearly the same set - costing 8.7 MiB of binary and nothing at runtime. A
  `-light` build is `main` with `queryChain` returning nil.
