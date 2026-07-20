# Ironwork

Distributed job scheduling and execution platform, built to demonstrate real
distributed-systems patterns end to end: Raft consensus, backpressure, CRDT
eventual consistency, transactional outbox, and full observability.

A **job** here is a unit of work placed and executed across worker nodes
(think Kubernetes scheduler or CI runner pool) — not cron time-scheduling.

> **Status: Phase 5 complete.** Two statemanager replicas hold an
> eventually-consistent job-statistics view built on hand-rolled CRDTs
> (grow-only counters + a last-writer-wins map), converging by push-pull
> gossip. The **dashboard at `localhost:8080/crdt`** makes it visible: submit
> jobs, **Partition** the replicas (gossip off) and watch their shard counts
> diverge, then **Heal** and watch them snap back to identical state — no
> lost increments. Earlier phases: Raft-led placement (`GET /raft`),
> backpressure + crash reassignment (`GET /workers`).

## Quickstart

Prerequisites: Docker + Compose, Go 1.23+, [buf](https://buf.build),
[mkcert](https://github.com/FiloSottile/mkcert), golangci-lint, make.

```sh
make certs   # REQUIRED FIRST: all service-to-service gRPC is mutual TLS —
             # nothing connects without this material (mkcert, gitignored)
make dev     # build + boot the full cluster
curl -s localhost:8080/health
```

Expected: HTTP 200 with every component `SERVING`:

```json
{
  "status": "SERVING",
  "checked_at": "…",
  "components": [
    {"name": "postgres-primary", "status": "SERVING"},
    {"name": "scheduler-1", "status": "SERVING"},
    {"name": "scheduler-2", "status": "SERVING"},
    {"name": "scheduler-3", "status": "SERVING"},
    {"name": "statemanager-1", "status": "SERVING"},
    {"name": "statemanager-2", "status": "SERVING"},
    {"name": "worker-1", "status": "SERVING"},
    {"name": "worker-2", "status": "SERVING"}
  ]
}
```

See the failure detection work: `docker compose stop worker-2` flips `/health`
to 503 with the failing component and reason; `docker compose start worker-2`
recovers it.

## Job API (Phase 1)

```sh
# Submit: sleeps 3s on a worker, then succeeds
curl -s -X POST localhost:8080/jobs \
  -d '{"name":"demo","payload":{"duration_ms":3000}}'
# → 201 {"id":"…","status":"scheduled","assigned_worker":"worker-1",…}

# Watch it run: pending → scheduled → running → succeeded
curl -s localhost:8080/jobs/<id>

# List (filter: status=pending|scheduled|running|succeeded|failed)
curl -s 'localhost:8080/jobs?status=running&limit=20'

# Ask for a failure to see the error path
curl -s -X POST localhost:8080/jobs -d '{"name":"doomed","payload":{"fail":true}}'
```

Payload contract (stub executor): `{"duration_ms": <0-60000>,
"fail": <bool>}`; empty payload sleeps 1s and succeeds. The flow:

```
POST /jobs
   └─ gateway (pure edge, no DB)
        └─ JobService.SubmitJob        (mTLS gRPC → whichever scheduler LEADS)
             │    followers reject FailedPrecondition; the gateway walks the
             │    set leader-first and caches whoever accepts
             └─ leader: INSERT jobs row (pending)
                  ├─ WorkerService.ExecuteJob   (round-robin + failover)
                  │    └─ worker: UPDATE scheduled → ack
                  │         └─ async: UPDATE running → sleep → succeeded/failed
                  └─ Raft log ← placement decision (replicates to all 3 FSMs)
GET /jobs/{id}
   └─ gateway → JobService.GetJob → any scheduler reads Postgres
```

## Consensus (Phase 3)

Raft (hashicorp/raft) across scheduler-1/2/3: mTLS transport on :9444 under
the same cluster CA as service gRPC, durable boltdb log + snapshots in a
volume per instance. Postgres stays the store of record for jobs — the Raft
log carries *placement authority*: who leads, and the replicated history of
placement decisions.

**Watch an election live** (terminal 1):

```sh
watch -n1 "curl -s localhost:8080/raft | python3 -m json.tool"
```

Then (terminal 2) kill whoever `/raft` names as leader and submit jobs while
it happens:

```sh
docker compose stop scheduler-2       # if scheduler-2 is the leader
curl -s -X POST localhost:8080/jobs -d '{"name":"failover-proof"}'
docker compose start scheduler-2
```

What you'll see: the two survivors elect a new leader at term+1 within ~2s;
the POST succeeds (gateway logs `routing to new leader`); the restarted node
rejoins as a *follower* at the current term and its `applied_index` and
`recent_placements` converge with the others. Every node's
`recent_placements` list is identical — that's the replicated log applied on
all three FSMs. Raft state is durable: restart the whole cluster and the
term and placement history carry over.

## Backpressure & crash recovery (Phase 4)

Workers execute at most `IRONWORK_CAPACITY` jobs (2 in compose) and reject
overflow with `ResourceExhausted`; every 2s they heartbeat liveness and load
to **all** schedulers, so each registry stays warm across leader failovers.
The leader only offers jobs to live, non-full workers (most headroom first),
and a leader-only *reaper* sweep (2s) retries `pending` jobs and reclaims
in-flight jobs from workers whose heartbeats died (8s TTL) — bounded by 3
execution attempts and a 60s placement budget for never-executed jobs.

**See backpressure** — flood more work than the pool can hold:

```sh
for i in $(seq 7); do
  curl -s -X POST localhost:8080/jobs -d '{"name":"flood","payload":{"duration_ms":6000}}' &
done; wait
curl -s localhost:8080/workers    # workers at 2/2, extras pending
# ...seconds later the reaper drains the queue: all 7 succeeded
```

**See crash reassignment** — kill a worker mid-job:

```sh
curl -s -X POST localhost:8080/jobs -d '{"name":"victim","payload":{"duration_ms":30000}}'
docker compose kill worker-1      # SIGKILL, no graceful drain
watch -n1 "curl -s localhost:8080/jobs/<id>"
```

Timeline: ~8s of `running worker-1` (heartbeats aging), then the reaper logs
`worker presumed dead; reclaiming jobs` and the job reappears as
`running worker-2, attempts: 2`, finishing there. `GET /workers` shows
`worker-1: alive=false` until its heartbeats resume.

## CRDT convergence (Phase 5)

The two statemanager replicas keep an **eventually-consistent statistics view**
of job outcomes — Postgres stays the source of truth; the statemanager is the
coordination-free *view* that stays writable under partition and always merges.
It is built on two hand-rolled CRDTs (`internal/crdt`, zero deps):

- a **G-Counter** per status and per worker — each replica increments only its
  own shard, so concurrent writes never conflict and `Merge` is the pointwise
  maximum (`succeeded → {statemanager-1: 4, statemanager-2: 4}`, value 8);
- an **LWW map** of recent outcomes — newer timestamp wins, ties break on
  replica id so every replica agrees.

`Merge` is commutative, associative, and idempotent (proved as law tests in
`internal/crdt`), so replicas converge regardless of gossip order or
repetition. Each worker reports its outcomes to **its own** replica
(worker-1 → statemanager-1, worker-2 → statemanager-2), and the replicas
push-pull gossip every second.

**Run the whole demo from `localhost:8080/crdt`** (buttons: Submit 5 jobs,
Partition, Heal), or from the CLI:

```sh
curl -s localhost:8080/crdt/state        # both replicas identical → CONVERGED
curl -s -X POST localhost:8080/crdt/partition   # gossip off on both replicas
for i in $(seq 6); do curl -s -X POST localhost:8080/jobs \
  -d '{"name":"split","payload":{"duration_ms":300}}' -o /dev/null; done
curl -s localhost:8080/crdt/state        # shards diverge → DIVERGED
curl -s -X POST localhost:8080/crdt/heal        # gossip back on
curl -s localhost:8080/crdt/state        # one gossip round later → CONVERGED
```

What you'll see: while partitioned, statemanager-1 sees `{sm-1: 4, sm-2: 2}`
and statemanager-2 sees `{sm-1: 2, sm-2: 4}` — each advanced only its own
shard, neither can see the other. After heal, both snap to `{sm-1: 4, sm-2: 4}`
within one gossip round (pointwise-max merge), with **no increment lost** — the
CRDT total then equals the Postgres terminal count. Kill and restart a replica
and it rejoins empty, catching up to full state in a single push-pull exchange
(state transfer *is* a merge).

## How the health check flows

```
curl :8080/health
   └─ gateway (REST, chi)
        └─ observer  HealthService.ClusterCheck   (mTLS gRPC)
             ├─ scheduler-1/2/3  HealthService.Check   (mTLS gRPC)
             ├─ worker-1/2       HealthService.Check   (mTLS gRPC)
             ├─ statemanager     HealthService.Check   (mTLS gRPC)
             └─ postgres-primary ping (pgx)
```

Three schedulers boot to form the future Raft quorum shape, but Phase 0
deliberately wires no election. Postgres runs as a primary + streaming
replica pair; only the primary is required in Phase 0.

## Commands

| Command      | What it does                                                |
|--------------|-------------------------------------------------------------|
| `make certs` | Generate cluster CA + service cert (prerequisite)           |
| `make dev`   | Boot the full cluster via compose                            |
| `make proto` | Regenerate `gen/` via buf **remote** plugins (no local protoc; needs network on first run) |
| `make build` | Compile all packages                                         |
| `make lint`  | golangci-lint                                                |
| `make test`  | All tests — the migration test runs real Postgres via testcontainers (needs Docker) |
| `make down`  | Stop cluster, drop volumes                                   |

The Postgres primary is published on host port **5433** (5432 is commonly
taken by other local stacks).

## Layout

```
proto/ironwork/v1/   contracts: JobService, WorkerService, StateService, HealthService
gen/                 committed buf codegen output — never hand-edit
cmd/                 gateway, scheduler, worker, statemanager, observer, migrate
internal/            config (viper), logging (zerolog), tlsutil, health/observer/gateway logic
migrations/          goose SQL — jobs table range-partitioned by created_at
deploy/postgres/     primary replication init + replica bootstrap scripts
```

## Phase plan

| Phase | Milestone | Visible proof |
|-------|-----------|---------------|
| **0 ✅** | Cluster boots; contracts compile; migrations apply | `/health` aggregates every component |
| **1 ✅** | One job through REST → gRPC → worker → Postgres (no scheduler in the middle) | job status transitions via API |
| **2 ✅** | Scheduler service in the placement path (single instance) | placement in scheduler-1 logs |
| **3 ✅** | **Raft across 3 schedulers** — election + failover (the hard milestone) | `GET /raft`: leader re-elected on kill, placement log identical on all nodes |
| **4 ✅** | Worker backpressure + heartbeat crash detection & reassignment | kill worker mid-job → job finishes elsewhere, `attempts: 2`; `GET /workers` liveness |
| **5 ✅** | CRDT statemanager | `localhost:8080/crdt` dashboard: Partition → shards diverge, Heal → reconverge, no lost increments |
| 6 | Transactional outbox dispatch | — |
| 7 | OTEL + Prometheus, frontend instrument panel | — |

Stack: Go 1.23 · gRPC + buf remote plugins · pgx v5 · goose v3 · chi ·
zerolog · viper · testify + testcontainers-go · golangci-lint.
