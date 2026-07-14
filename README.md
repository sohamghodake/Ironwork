# Ironwork

Distributed job scheduling and execution platform, built to demonstrate real
distributed-systems patterns end to end: Raft consensus, backpressure, CRDT
eventual consistency, transactional outbox, and full observability.

A **job** here is a unit of work placed and executed across worker nodes
(think Kubernetes scheduler or CI runner pool) — not cron time-scheduling.

> **Status: Phase 2 complete** — the scheduler is in the placement path. The
> gateway is now a pure REST↔gRPC edge (no database, no placement); jobs go
> gateway → scheduler-1 (JobService) → worker → Postgres. Single scheduler
> instance places jobs; Raft election across all three is Phase 3.

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
    {"name": "statemanager", "status": "SERVING"},
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
        └─ JobService.SubmitJob            (mTLS gRPC → scheduler-1)
             └─ scheduler: INSERT jobs row (pending)
                  └─ WorkerService.ExecuteJob   (round-robin + failover)
                       └─ worker: UPDATE scheduled → ack
                            └─ async: UPDATE running → sleep → succeeded/failed
GET /jobs/{id}
   └─ gateway → JobService.GetJob → scheduler reads Postgres
```

Watch placement decisions land: `docker compose logs -f scheduler-1 | grep placed`.

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
| 3 | **Raft across 3 schedulers** — election + failover (the hard milestone) | leader visibly re-elected on kill |
| 4 | Worker backpressure + heartbeat crash detection & reassignment | on-screen reassignment |
| 5 | CRDT statemanager | divergence/reconvergence visualization |
| 6 | Transactional outbox dispatch | — |
| 7 | OTEL + Prometheus, frontend instrument panel | — |

Stack: Go 1.23 · gRPC + buf remote plugins · pgx v5 · goose v3 · chi ·
zerolog · viper · testify + testcontainers-go · golangci-lint.
