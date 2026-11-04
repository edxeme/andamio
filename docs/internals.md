# Internals

How andamio works, why it works that way, and where it cuts corners. Read this before extending the protocol.

## One datagram, one message

Every UDP datagram carries exactly one gob-encoded `proto.Message`. There is no framing, no fragmentation and no compression: a message is either smaller than ~64 KB or it is a bug. Membership tables that outgrow a datagram would need a real anti-entropy mechanism (see *Simplifications*).

Why gob? It is stdlib, handles type evolution decently, and I did not want hand-rolled varints distracting from the protocol logic. A production system would probably pick protobuf for cross-language support.

## Probe rounds

Each interval, a node picks one random reachable peer and probes it in three stages:

1. **Direct ping.** Send `Ping{seq}`, wait `ProbeTimeout` for `Ack{seq}`. Sequence numbers are process-local and monotonically increasing; late acks resolve nothing because the registration is deleted when the round ends.
2. **Indirect ping.** If the direct ping times out, the prober picks up to `Witnesses` random *other* alive peers and sends `PingReq{seq, target, targetAddr}`. Each witness pings the target directly and relays the outcome as a plain `Ack{seq}` back to the prober. Indirect probing is what makes SWIM survive asymmetric packet loss: A might not hear from B, but C probably can.
3. **Suspicion.** If both stages fail, the target is reported as `Suspect`. This report is gossiped like any other. A suspect that fails to refute within `SuspectMult` probe intervals is promoted to `Dead`.

Timing budget: a worst-case round takes `ProbeTimeout + 2*ProbeTimeout` (the indirect stage waits two windows, to cover the witness's own ping attempt). The probe loop is deliberately sequential — one peer per round — because the SWIM paper's weak completeness guarantees hold as long as every reachable member is eventually probed. Parallel probing would buy faster detection at the cost of much noisier interference between rounds.

## Merge rules and incarnations

The membership table folds reports under two ordering rules:

- Higher **incarnation** wins, always. Incarnations are per-node counters.
- With equal incarnations, `suspect` beats `alive`, and `dead`/`left` beats both. This ordering is what turns gossip that arrives out of order into eventual agreement instead of flapping.

The incarnation mechanism resolves the classic ambiguity of failure detectors: if a live node gets suspected (because one observer lost packets), the node sees a suspicion report about itself and **refutes** it — it bumps its incarnation and re-announces itself alive. Only nodes that cannot speak for themselves ever die.

Boot time seeds the initial incarnation, so a node that restarts with the same name automatically outranks every stale record about its past self.

## Gossip piggybacking

There is no gossip timer and no gossip message. Instead, each outbound message (ping, ack, join-ack, anything) carries up to `gossipFanout` pending changes, and each change carries a retransmit budget (`changeRetransmits`). A change leaves the queue when its budget runs out; a new report about the same member replaces the queued one and refreshes the budget.

This bounds message size during churn: with fanout 8 and budget 6, a change reaches a 3-node cluster several times over before being retired, and a 100-node cluster still only pays 8 members' worth of bytes per datagram.

## Graceful leave

`Leave` marks the node `left` in its own table, queues that report with a triple retransmit budget, and sends a direct `Leave` message to every alive peer before shutting down. Graceful departures spread faster than suspicion-based deaths and do not trigger refutation logic — the node is not coming back to defend itself.

## Simplifications

Deliberate cuts, in order of how much I want to fix them:

1. **No authentication.** Any process that can send a datagram can join the cluster or forge reports. memberlist ships with encryption and MACs for a reason.
2. **No prioritized probing.** The paper suggests probing recently-joined and recently-suspected nodes more often. Random pick is simpler and still eventually complete.
3. **Dead members are never pruned.** They stop being probed but stay in the table forever, so very long-lived clusters would need a tombstone TTL.
4. **No state persistence.** A restarting node relearns the world from its seed. Fine for membership; wrong for any stateful layer built on top.
5. **UDP only.** Real deployments would negotiate TCP for large anti-entropy transfers.

## Testing approach

The tests run real UDP traffic on loopback with accelerated clocks (`ProbeInterval=100ms`). Formation, hard-kill detection, graceful leave and refutation are all asserted through eventual-state polls, not sleeps — which keeps them stable under load while still exercising the actual timers. Everything runs under `-race` in CI.

## What I would do differently

Hand-rolling the ack registration was the right call for learning, but `acks map[uint64]chan struct{}` has grown tendrils into every code path. A second iteration would separate "transport" (bytes in/out) from "protocol" (rounds, suspicion) more sharply, and the suspicion expiry would be driven by an explicit state-machine transition table instead of a scan in the ticker.
