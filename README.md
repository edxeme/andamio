# andamio

*andá·mio* — **scaffolding**: the temporary structure that holds everything up while the real thing gets built.

A SWIM-style cluster membership library for Go, with zero dependencies. Every node runs a probe loop over UDP, gossips membership updates piggybacked on any outbound message, and survives packet loss through indirect probes and suspicion lifecycles — the same primitives that power service meshes and coordinator services, written from scratch to understand them properly.

I spent twenty years making sure structures could carry their loads; this is the distributed-systems version of that exercise. The goal is not to replace [hashicorp/memberlist](https://github.com/hashicorp/memberlist) — it is to be able to explain, line by line, how such systems earn your trust.

## What's inside

- **Probe protocol** — direct ping, then `ping-req` through `k` random witnesses, then suspicion. Failures are never declared on a single lost packet.
- **Gossip** — every ping, ack and status reply carries up to 8 pending membership changes, each with a retransmit budget, so updates spread without a dedicated control plane.
- **Merge rules** — incarnation numbers order conflicting reports; on a tie, suspicion beats health and termination beats suspicion. A node that is wrongly suspected refutes the report by bumping its own incarnation.
- **Status API** — `/members`, `/self` and `/healthz` per node, plus `andamioctl` to read the table from the terminal.
- **Ops artifacts** — a scratch-based container image, a five-node compose mesh, and StatefulSet manifests with DNS seeding for Kubernetes.

## Quickstart

Build and run a three-node cluster on loopback:

```sh
go build -o bin/andamio ./cmd/andamio
go build -o bin/andamioctl ./cmd/andamioctl

./bin/andamio -name n1 -bind 127.0.0.1 -port 7946 &
./bin/andamio -name n2 -bind 127.0.0.1 -port 7947 -seed 127.0.0.1:7946 &
./bin/andamio -name n3 -bind 127.0.0.1 -port 7948 -seed 127.0.0.1:7946 &

./bin/andamioctl members 127.0.0.1:7373
```

Kill any node with `kill -9` and watch the survivors move it to `suspect`, then `dead` — no operator, no coordinator, just the protocol.

### Docker

```sh
docker build -t edxeme/andamio:dev -f deploy/docker/Dockerfile .
docker compose -f deploy/docker/docker-compose.yml up -d
andamioctl members localhost:7371
```

### Kubernetes

```sh
kubectl apply -f deploy/k8s/
```

The headless service gives every replica a stable DNS name; pods join through `andamio-0.andamio.default.svc.cluster.local:7946`.

## Tests

```sh
go test -race ./...
```

The cluster tests spin real UDP nodes on loopback: formation through a seed, hard-kill detection, graceful leave, and self-refutation under a forged suspicion report.

## Design notes

The protocol decisions, simplifications and roadmap are written up in [`docs/internals.md`](docs/internals.md) — including what I would do differently next time.

## Status

Working prototype, test suite clean under the race detector. Not battle-tested, not authenticated, not a library you should bet a company on. It *is* a good map of the terrain.

## License

[MIT](LICENSE)
