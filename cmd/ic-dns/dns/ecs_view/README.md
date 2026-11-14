# ecs_view

## Name

_ecs_view_ - serves DNS records filtered by who is asking.

## Description

_ecs_view_ holds an in-memory record set and answers each query with only what
that querier may see. It sources nothing itself: another plugin in the same
server block builds the records and publishes them here.

A querier is placed on a set of networks, and sees only the names reachable on
those networks, with only their addresses there. Two clients on different
networks asking the same name get different answers, and a multi-homed host is
always answered on the network it shares with the client - so every answer holds
an address the querier can actually reach.

The querier is identified two ways, in this order:

1. The [EDNS0 Client Subnet](https://datatracker.ietf.org/doc/html/rfc7871)
   option, which a forwarding resolver supplies. dnsmasq does it with
   `add-subnet=32,128`.
2. The query's source address, when no client subnet is present.

The order matters. A resolver that forwards on a client's behalf puts its own
address on the packet, so the client subnet is the only truth when one is there.
Point a client's `resolv.conf` straight at ic-dns and nothing relays, so the
source address is the querier.

**It fails closed.** A querier that lands on no known network answers NXDOMAIN,
because there is no view to answer it from that would not hand out addresses it
cannot route to. A name that exists but the querier cannot reach answers
NXDOMAIN too, rather than NODATA, because that is the answer shape matching
"there is nothing here for you".

**None of this is a security boundary.** A UDP query's source address is
forgeable and a client subnet is asserted by whoever attaches one, so the rule
is what a querier can reach and not what it is allowed to see. Reaching the
server is the real boundary - see [Caveats](#caveats).

## Syntax

```
ecs_view {
    echo_subnet
}
```

- `echo_subnet` echoes the EDNS0 Client Subnet option on the reply, with a
  scope. Off by default; see [Client Subnet Echo](#client-subnet-echo).

Everything it serves comes from the sources in the same server block, and each
of those carries its own configuration, so `ecs_view` on its own is the usual
form.

Use `.` as the server block rather than a fixed zone. A source can name its
zones at runtime, so the zone list is known only then. Names outside every known
zone fall through to the next plugin.

## Sources

A source is any plugin in the same server block that implements:

```go
// Source is a plugin that feeds views to ecs_view.
type Source interface {
    // SetSink is called once at startup, before the source produces anything.
    SetSink(Sink)
}
```

_ecs_view_ finds them by walking the server block at startup, the same way
_ready_ and _metadata_ find theirs. A source then publishes whole snapshots:

```go
// Sink is where a source publishes.
type Sink interface {
    // Replace swaps everything served for snap, which holds every zone the
    // source serves.
    Replace(snap *Snapshot)
    // SetHealthy says whether the source's data is currently fresh. The
    // first true also turns the view warm, for good.
    SetHealthy(healthy bool)
}
```

Publishing is `Replace` semantics: a zone that stops appearing is deleted by
absence. The snapshot handed over is finished - serials stamped, records
rendered - and swapping it in is a pointer store. _ecs_view_ derives nothing and
accumulates nothing; it is a read-only view onto what the source published.

[_incus_](../incus/README.md) is that source.

## Partial Zones

A zone may be marked `Fallthrough`, which makes it claim the names in it and
nothing else: a query for a name it does not hold goes to the next plugin
instead of answering NXDOMAIN.

It is for a zone a source invented rather than one anybody asked to serve - the
case being an alias like `me.example.com.` on an instance, which has to be
answered from _some_ zone. Claiming `example.com.` outright would have that one
name blackhole every other name in the domain, for every querier.

Held is the test, not visible. A name the zone does hold is answered under the
usual rule, invisible included, so falling through cannot be used to tell a
hidden name from an absent one. The apex of such a zone is not held either, so
no SOA is synthesized for a domain this server does not serve.

## Aliases

A name may answer with a CNAME onto another, which `RenderCName` builds. The
chase is done when the snapshot is built rather than per query: the CNAME goes
into the A and AAAA sets as well as its own, canonical name first, so answering
through one costs what answering anything else costs.

One CNAME value serves every network the name is reachable on, since it is the
one record that does not vary by network, and it heads every type it was chased
into. `Gather` drops the repeat by identity, which is the only de-duplication a
join does: everything else genuinely differs per network.

Nothing here knows what an alias is. A source decides which names get one and
how far they reach, by rendering them under the network keys it chooses - which
is what makes them obey the same visibility rule as everything else.

## Zone Serials

Each zone carries a serial that moves when that zone's records move, and at no
other time. Republishing identical records leaves it alone, so a secondary
polling the SOA re-transfers on a real change and on nothing else.

The source stamps it, because the source is the only thing that knows what it
published last. Deriving one from the clock, as Incus's own network zones do,
re-transfers on every publish whether or not anything moved.

A restart is the same question over a longer gap, and it is the source's to
answer too: this plugin holds nothing across one. _incus_ carries its serials in
`data_dir`; a source that does not restarts every zone at 1.

## Client Subnet Echo

With `echo_subnet`, a query that carries a client subnet gets one back, as
[RFC 7871](https://datatracker.ietf.org/doc/html/rfc7871) requires: FAMILY,
SOURCE PREFIX-LENGTH and ADDRESS copied, and SCOPE PREFIX-LENGTH saying how
specific the answer is.

| answer                                         | scope    |
| ---------------------------------------------- | -------- |
| A, AAAA, PTR, NODATA, the fail-closed NXDOMAIN | 32 / 128 |
| apex SOA and NS                                | 0        |

Scope is the only signal a cache has for how widely an answer may be shared, and
no option at all reads as "this answer was not tailored". A view-dependent
answer is scoped to the querier's whole address, so a conformant cache keys it
per client and shares it with nobody; the apex is identical for every querier
and says so.

A query with no client subnet gets no option back. The source address identified
the querier, and nothing asserted a subnet to be answered about.

**Turn it on when an ECS-aware cache sits before ic-dns**, and leave it off
otherwise. Nothing else reads the field, while putting the option on the reply
and packing it costs the query path allocations on every query that carries a
subnet - which, where a resolver forwards, is all of them.

Without it the answers are the same answers: the option decides who is asking
either way, and only the reply is silent about how far it travels.

## TTL and Staleness

Records are rendered by the source with the TTL it was configured for, and the
snapshot carries it. While the source reports itself unhealthy, answers are
clamped to 5 seconds so stale records expire fast once a client can reach a
healthy server.

The clamp is deliberately coarse: it applies to every zone, not only the ones
affected. It errs toward shorter TTLs.

## Before The First Read

Until its source first reports itself healthy, the view is cold: what it holds
may be only part of what exists, so a miss may be a name that has not been read
yet rather than one that is not there. Every miss is then answered `SERVFAIL`,
with the [RFC 8914](https://datatracker.ietf.org/doc/html/rfc8914) extended
error `Not Ready` (14) when the query carries EDNS (none when it does not):

- a name a zone does not hold, a name the querier cannot see, and a querier on
  no known network - the three answers that are `NXDOMAIN` once warm. A name
  two instances claim as an alias reads as one the querier cannot see, for a
  trusted querier as for anybody: the claimant that has to go may only not have
  been read yet;
- a name under one of `Claims` that no zone holds yet, which would otherwise
  fall through to the next plugin. A source sets `Claims` to the domains it
  serves before any zone in them has been published: for _incus_, the suffix
  project zones are built under.

Hits, `NODATA` and the apex are answered as usual, clamped while unhealthy.
Anything outside every zone and every claim falls through as usual, so names on
the internet keep resolving while the fleet is being read.

Healthy means the source says it has been all the way round, not that it read
everything: _incus_ reports it at the end of a round that may have skipped a
project whose listing failed, or an instance whose read timed out. The view
turns warm with those holes, and from then on they are `NXDOMAIN` until a later
round fills them.

A claim is the whole domain. Where another server answers part of it - a second
`ic-dns` serving other projects under the same suffix, reached by forwarding -
those names are `SERVFAIL` here too until this view is warm, instead of falling
through to it.

`SERVFAIL` because, unlike `NXDOMAIN`, the common stub resolvers do not take it
as final where another server is listed. What they do with it, measured:

| resolver | the first of two servers `SERVFAIL`s      | the only server `SERVFAIL`s                         |
| -------- | ----------------------------------------- | --------------------------------------------------- |
| glibc    | asks the next, answered in ~0.02 s        | `EAI_AGAIN` at once, each `search` domain tried     |
| musl     | asks both at once, ignores the `SERVFAIL` | `EAI_AGAIN` after 5 s, at the first `search` domain |
| Go       | temporary error, asks the next            | temporary error                                     |

systemd-resolved has not been measured. A forwarder's `NXDOMAIN` for a name it
has never heard of is final for all three, may be cached negatively, and is
wrong. With two servers listed, a cold one costs a client nothing measurable.
With a single cold server it costs a temporary failure instead of a wrong
answer, and on musl every such miss blocks for 5 s first. A hidden name and an
absent one still answer alike.

Warm latches. A source that reports unhealthy again - _incus_ does when its
event stream drops - leaves the view warm: the zones are as full as when it last
confirmed them, the TTL clamp already says they are stale, and an Incus outage
drops the stream of every replica at once, so failing misses then would fail
them everywhere at the same time.

## Metrics

With the _prometheus_ plugin enabled:

- `coredns_ecs_view_requests_total{server,result}` - requests by result, where
  result is `success`, `nodata`, `nxdomain`, `denied` or `servfail` (a miss
  answered before warm, see [Before The First Read](#before-the-first-read)).
- `coredns_ecs_view_unidentified_clients_total{server}` - requests refused
  because the querier could not be placed on any known network.
- `coredns_ecs_view_zones` - zones currently served.
- `coredns_ecs_view_addresses` - addresses indexed for client identification.

An address claimed by more than one host identifies no querier and is refused
here, but detecting the clash belongs to the source - see
`coredns_incus_ambiguous_addresses`.

## Ready

This plugin reports readiness to the _ready_ plugin. It turns ready once a
source has published a snapshot and reports itself healthy - which is also the
moment it turns warm.

An empty fleet publishes an empty snapshot, which is not the same as never
having published: only one of those is ready.

## Caveats

**`cache` must stay below `ecs_view`.** This plugin writes its own reply and
never calls the next plugin on an in-zone hit, so `cache` never sees an answer
that varies per querier. Putting `cache` first would cache one client's view and
serve it to another, and the scope on the reply would not save it: CoreDNS's
`cache` does not read the client subnet.

**A client subnet is asserted by whoever sends it.** Anything that can reach the
server can claim to be any client by attaching one. Reaching the server is the
real boundary, which is why _ecs_view_ belongs on a network only its clients are
on.

## Examples

Serve Incus instances, per querier:

```corefile
.:53 {
    ecs_view
    incus https://10.0.0.1:8443 {
        token_file /run/secrets/token
    }
    forward . 10.0.0.1
    errors
    log
}
```

## Also See

[RFC 7871](https://datatracker.ietf.org/doc/html/rfc7871) for EDNS0 Client
Subnet, and [docs/ecs_view.md](../../docs/ecs_view.md) for how the views are
built.
