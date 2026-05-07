# GojoDB Documentation

GojoDB is a distributed, horizontally-scalable key-value database written in Go. It combines Raft-based cluster coordination, a pluggable multi-index storage layer (B-tree, inverted index, R-tree spatial), write-ahead logging with log replication, and tiered storage management to provide a durable, query-rich data platform.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Subsystems](#subsystems)
   - [Storage Engine](#storage-engine)
   - [Write Engine](#write-engine)
   - [Indexing Layer](#indexing-layer)
   - [Query Engine](#query-engine)
   - [Transaction Manager](#transaction-manager)
   - [Replication](#replication)
   - [Sharding & Coordination](#sharding--coordination)
   - [Controller](#controller)
   - [Security](#security)
   - [Resource Management](#resource-management)
   - [Data Management](#data-management)
   - [API Layer](#api-layer)
3. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Build](#build)
   - [Run a 3-Node Cluster](#run-a-3-node-cluster)
   - [Docker Compose](#docker-compose)
4. [Configuration](#configuration)
5. [Client SDK](#client-sdk)
6. [Query Language (GQL)](#query-language-gql)
7. [Observability](#observability)
8. [Project Layout](#project-layout)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Clients  (CLI · Go SDK · gRPC · GraphQL · HTTP)                 │
└────────────────────────┬─────────────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │   Gateway / Router   │  ← shard-aware request routing
              └──────────┬──────────┘
                         │
        ┌────────────────┴─────────────────┐
        │                                  │
┌───────▼────────┐                ┌────────▼──────────────────────┐
│  Controller    │  Raft cluster  │  Storage Nodes  (1 … N)       │
│  Cluster       │◄──────────────►│  ┌───────────────────────┐    │
│  · Leader      │                │  │ B-tree index          │    │
│    election    │                │  │ Inverted index        │    │
│  · Shard map   │                │  │ Spatial R-tree        │    │
│  · Node health │                │  │ WAL + Buffer Pool     │    │
└───────┬────────┘                │  │ Transaction Manager   │    │
        │                         │  │ Log Replication Mgr   │    │
        │  shard assignment        │  └───────────────────────┘    │
        └─────────────────────────┘                               │
                                                                   │
                          ┌────────────────────────────────────────┘
                          │
              ┌───────────▼──────────────┐
              │   Tiered Storage          │
              │  ├─ Hot  (local disk/EFS) │
              │  └─ Cold (S3 / GCS)      │
              └──────────────────────────┘
```

**Request flow (write)**

1. Client sends a gRPC `Put` to the Gateway.
2. Gateway hashes the key to a slot and forwards to the primary storage node for that slot.
3. The storage node appends a WAL record, updates the in-memory buffer pool, then replies `OK`.
4. The Replication Manager streams the WAL record to all replicas for the slot.
5. Tiered Storage Manager periodically migrates cold data to object storage per DLM policy.

---

## Subsystems

### Storage Engine

**Path**: `core/storage_engine/`

Abstracts multi-tier data placement. Three tiers are supported:

| Tier | Adapter | Characteristics |
|------|---------|-----------------|
| Hot | `FileStoreAdapter` / `EFSAdapter` | Low-latency local or NFS-mounted disk |
| Cold | `S3Adapter` / `GCSAdapter` | High-durability object storage; higher latency |
| Tiered | `TieredStorageManager` | Policy-driven automatic migration between hot and cold |

**Tiered Storage Manager** evaluates Data Lifecycle Management (DLM) policies on a configurable schedule. Each policy specifies:
- Source and target tiers
- Entity type (e.g., WAL segments, index snapshots)
- Schedule (cron-like interval)
- Retention/age thresholds

Cluster-wide tiering metadata is committed through the Raft FSM so all nodes agree on which data has been tiered.

---

### Write Engine

**Path**: `core/write_engine/`

Provides durable, high-throughput writes with crash recovery.

#### Write-Ahead Log (WAL)

- **Segment-based**: Default segment size 16 MB; old segments archived to cold storage.
- **Log record types**: `INSERT_KEY`, `DELETE_KEY`, `UPDATE`, `CHECKPOINT`, `BEGIN`, `PREPARE`, `COMMIT`, `ABORT`, `RTREE_INSERT`, `RTREE_DELETE`, …
- **Replication slots**: Per-replica LSN tracking enables safe segment pruning without losing in-flight replication data.
- **Encryption**: AES-GCM symmetric encryption supported via `CryptoUtils`.
- **Recovery**: LSN-based ARIES-style redo on startup.

#### Buffer Pool Manager

- Fixed-size page frames (default 4 096 bytes) backed by an LRU eviction policy.
- Pin-count reference tracking prevents eviction of hot pages.
- Page-level read/write latches (RWMutex) for physical concurrency control.
- Dirty pages flushed to disk on eviction or explicit checkpoint.

#### Disk Manager (Flush Manager)

- Manages raw file I/O; stores magic header (`0x6010DB00`), page size, root page ID, and free-list head in the file header.
- CRC32 checksums on every page for integrity verification.

---

### Indexing Layer

**Path**: `core/indexing/`

Three pluggable index engines, all sharing a common `IndexManager` interface (`core/indexmanager/indexmanager.go`).

#### B-tree Index (`btree/`)

- Variable-degree, disk-backed B-tree using the Buffer Pool Manager for page I/O.
- Supports point lookups, ordered range scans, and reverse scans.
- Can participate in two-phase commit as a transaction participant.
- Root page tracked in file header; changes logged as `LogTypeRootChange`.

#### Inverted Index (`inverted_index/`)

- In-memory term dictionary mapping each term to a `PostingsListMetadata`.
- Postings lists stored on disk as page chains; dictionary flushed on close, recovered on open.
- Supports document insertion, keyword search with tokenization, and deletion.

#### Spatial Index (`spatial/`)

- R-tree implementation with minimum bounding rectangles (MBR).
- Each node holds up to 4 entries (configurable) for 2-D bounding-box queries.
- Supports `Insert(rect, data)`, `Search(rect)`, and `Delete(rect, data)`.
- Used for `NEAR(lat, lon, radius)` and `WITHIN(minLat, minLon, maxLat, maxLon)` queries.

---

### Query Engine

**Path**: `core/query_engine/`

#### Parser (`parser/`)

A hand-written recursive-descent lexer + parser for **GQL** — GojoDB's query dialect. See the [Query Language](#query-language-gql) section for full syntax.

#### Optimizer (`optimizer/`)

Rule-based optimizer converts a parsed AST into a physical `Plan` tree:

| Predicate type | Plan chosen |
|----------------|-------------|
| `key = 'x'` | `PlanTypeBtreeScan` (point lookup) |
| `key BETWEEN 'a' AND 'b'` | `PlanTypeBtreeScan` (range) |
| `CONTAINS(field, 'text')` | `PlanTypeInvertedScan` |
| `NEAR(...)` / `WITHIN(...)` | `PlanTypeSpatialScan` |
| No predicate | Full `PlanTypeBtreeScan` |

Residual predicates (not absorbed by the index) are wrapped in a `PlanTypeFilter` node.

#### Executor (`executor/`)

Executes a physical plan bottom-up, routing each node type to the appropriate `IndexManager`. Returns a `[]Row` result set.

---

### Transaction Manager

**Path**: `core/transaction/`

Provides ACID semantics with distributed two-phase commit (2PC).

```
Begin() → [Read/Write operations] → Prepare() → Commit()
                                              └─ Abort()
```

- **Lock Manager**: Row-level shared/exclusive locks with deadlock detection via a waits-for graph. Supports lock upgrade (shared → exclusive).
- **ARIES Recovery**: On startup, the transaction manager replays committed WAL records (redo pass) and rolls back uncommitted transactions (undo pass).
- **2PC log records**: `PREPARE`, `COMMIT`, `ABORT` records written to WAL before sending the decision to participants.

---

### Replication

**Path**: `core/replication/`

#### Raft Consensus (`raft_consensus/`)

Uses [HashiCorp Raft](https://github.com/hashicorp/raft) for cluster metadata. The `FSM` applies commands to shared cluster state:

- Node registry: register/deregister storage nodes.
- Shard assignment: assign slot ranges to primary/replica nodes.
- Tiering metadata: record which data has been migrated to cold storage.
- Migration state: track shard migration progress.

#### Log Replication (`log_replication/`)

Each index type has a dedicated replication manager implementing `ReplicationManagerInterface`:

| Manager | Index type |
|---------|------------|
| `BTreeReplicationManager` | B-tree page changes |
| `InvertedIndexReplicationManager` | Inverted index updates |
| `SpatialReplicationManager` | R-tree spatial changes |

**Replication flow:**
1. Primary writes an index page → WAL record created.
2. Replication Manager sends the WAL record over an event stream (HTTP/QUIC) to all slot replicas.
3. Replica deserialises the record and calls `ApplyLogRecord` on its local index.
4. Replica advances its LSN; the primary's replication slot tracks per-replica progress.

The **ReplicationOrchestrator** coordinates all per-index replication managers and handles primary/replica role transitions.

---

### Sharding & Coordination

**Path**: `core/sharding/`

GojoDB uses **hash-slot sharding** (default 16 384 slots, FNV-1a hash). Key routing:

```
slot = FNV1a(key) % 16384
primary_node = metadata_store.PrimaryForSlot(slot)
```

#### Coordinator (`coordinator/`)

Drives slot assignment and migration through Raft-committed commands:

- `AssignSlotRange(start, end, primary, replicas)` — maps a range of slots to a node.
- `BeginMigration / AdvanceMigration / CompleteMigration` — orchestrates live shard moves.

#### Metadata Store (`metadata_store/`)

Wraps the Raft FSM's `SlotRangeInfo` structures and caches slot→primary mappings locally for O(1) routing lookups without a Raft round-trip.

---

### Controller

**Path**: `core/controller/` and `cmd/gojodb_controller/`

The controller cluster manages topology and health.

| Component | Purpose |
|-----------|---------|
| **Leader Election** | Wraps Raft leadership state; publishes `LeadershipChange` events to subscribers. |
| **Node Discovery** | TTL-based heartbeat registry; background reaper expires silent nodes. |
| **Resource Scheduler** | Assigns shards to least-loaded nodes; triggers rebalancing when topology changes. |
| **HTTP API** | Endpoints: `/assign_slot_range`, `/status`, `/heartbeat` (storage nodes POST here). |

Storage nodes POST a heartbeat to the controller every few seconds. The controller marks a node unhealthy after 15 s of silence.

---

### Security

**Path**: `core/security/`

#### Authentication (`authentication/`)

- **JWT (HMAC-SHA256)**: `Authenticator.IssueToken(subject, roles)` → compact JWT.
- **Validation**: `ValidateToken` verifies signature and expiry; injects `StandardClaims` into `context`.
- **gRPC interceptors**: `UnaryInterceptor` and `StreamInterceptor` validate the `Authorization: Bearer <token>` header from gRPC metadata.

#### Authorization (`authorization/`)

- **RBAC**: Three built-in roles — `admin`, `writer`, `reader`.
- **Policy engine**: Each `Policy` maps a role to a set of `Action` values and resource name patterns.
- **gRPC interceptors**: Extract claims from context; look up required action per gRPC method; deny with `PermissionDenied` if not permitted.

Built-in role permissions:

| Role | Permitted actions |
|------|------------------|
| `admin` | All |
| `writer` | read, write, delete, bulk_write, text_search, spatial |
| `reader` | read, text_search, spatial |

#### Encryption (`encryption/`)

- **AES-GCM**: `CryptoUtils` for symmetric encryption of WAL records and stored data.
- **Envelope Encryption** (`kms_integration/`): Wraps a short-lived data-encryption key (DEK) under a key-encryption key (KEK) supplied by a `KEKProvider`. `LocalKEKProvider` stores keys in-process; swap it for an AWS KMS / GCP Cloud KMS / HashiCorp Vault provider in production.

---

### Resource Management

**Path**: `core/resource_management/`

#### Admission Control (`admission_control/`)

Per-tenant token-bucket rate limiter.

```go
ctrl := admission_control.NewController(admission_control.TenantConfig{Rate: 100, Burst: 200})
ctrl.LoadConfig("tenant-A", admission_control.TenantConfig{Rate: 500, Burst: 1000})
if err := ctrl.AllowCtx(ctx, "tenant-A"); err != nil {
    // 429 Too Many Requests
}
```

#### Query Throttling (`query_throttling/`)

Per-query deadline enforcement and slow-query logging.

```go
t := query_throttling.NewThrottler(query_throttling.Config{
    DefaultTimeout:    5 * time.Second,
    SlowQueryThreshold: 200 * time.Millisecond,
    MaxSlowQueryLog:   1000,
})
ctx, cancel := t.WithQueryTimeout(ctx, "tenant-A")
defer cancel()
```

#### User Quotas (`user_quotas/`)

Per-namespace storage and request-rate quotas.

```go
mgr := user_quotas.NewManager()
mgr.SetQuota("ns-1", user_quotas.Quota{
    MaxStorageBytes:      10 << 30, // 10 GiB
    MaxRequestsPerSecond: 1000,
    MaxKeysCount:         1_000_000,
})
if err := mgr.CheckAndRecordWrite("ns-1", valueLen); err != nil {
    // quota exceeded
}
```

---

### Data Management

**Path**: `data_management/`

#### Backup & Restore

Snapshot-based backup of index state. Integrates with the `IndexManager` snapshot API (`PrepareSnapshot`, `StreamSnapshot`).

#### Archival (`archival/`)

Policy-driven archival of old data:

```go
policy := archival.ArchivePolicy{
    Name:       "archive-old-keys",
    KeyPrefix:  "log:",
    MaxAgeHours: 720, // 30 days
    Sink:       archival.NewLocalArchiveSink("/mnt/archive"),
}
job := archival.NewArchiveJob(indexMgr, policy)
job.Run(ctx)
```

After archival, the job deletes matched keys from the hot index.

#### Schema Migration (`migration/schema_migrator/`)

Versioned, idempotent migration system:

```go
m := schema_migrator.NewMigrator()
m.Register(schema_migrator.Migration{
    Version: 1,
    Name:    "add-user-prefix",
    Up: func(ctx context.Context, ex schema_migrator.Executor) error {
        // transform data
        return nil
    },
    Down: func(ctx context.Context, ex schema_migrator.Executor) error {
        // revert
        return nil
    },
})
m.Migrate(ctx, executor, schema_migrator.Up)
```

---

### API Layer

**Path**: `api/`

#### gRPC API (`proto/api.proto`)

Core operations exposed over gRPC:

| RPC | Description |
|-----|-------------|
| `Put(PutRequest)` | Insert or overwrite a key-value pair |
| `Get(GetRequest)` | Point lookup |
| `Delete(DeleteRequest)` | Delete by key |
| `GetRange(GetRangeRequest)` | Ordered range scan |
| `TextSearch(TextSearchRequest)` | Full-text search via inverted index |
| `BulkPut(BulkPutRequest)` | High-throughput batch write |
| `BulkDelete(BulkDeleteRequest)` | Batch delete |

#### HTTP / GraphQL

The `gojodb_server` binary also exposes an HTTP server (default `:8080`) used for:
- GraphQL queries (`graphql_service/`)
- Replication handshakes and event streams
- Health checks

#### Gateway (`cmd/gojodb_gateway/`)

A stateless routing layer that:
1. Fetches the current shard map from the controller.
2. Hashes the request key to determine the target slot and primary node.
3. Forwards the gRPC call to the appropriate storage node.
4. Retries on primary failure after a configurable backoff.

---

## Getting Started

### Prerequisites

- **Go 1.24+**
- **Docker & Docker Compose** (optional, for multi-node local cluster)
- Ports available: `6000–6003` (replication), `7000–7003` (Raft), `8000–8003` (gRPC), `8080–8083` (HTTP), `9000–9003` (heartbeat)

### Build

```bash
git clone https://github.com/sushant-115/gojodb.git
cd gojodb
go mod tidy
go build -o bin/gojodb_server    ./cmd/gojodb_server
go build -o bin/gojodb_controller ./cmd/gojodb_controller
go build -o bin/gojodb_gateway   ./cmd/gojodb_gateway
go build -o bin/gojodb_cli       ./cmd/gojodb_cli
```

### Run a 3-Node Cluster

**Node 1 — bootstrap leader:**
```bash
./bin/gojodb_server \
  -node_id=node1 \
  -raft_addr=127.0.0.1:7001 \
  -grpc_addr=127.0.0.1:8001 \
  -http_addr=127.0.0.1:8081 \
  -replication_addr=127.0.0.1:6001 \
  -heartbeat_addr=127.0.0.1:9001 \
  -raft_dir=/tmp/gojodb/node1 \
  -bootstrap=true
```

**Node 2:**
```bash
./bin/gojodb_server \
  -node_id=node2 \
  -raft_addr=127.0.0.1:7002 \
  -grpc_addr=127.0.0.1:8002 \
  -http_addr=127.0.0.1:8082 \
  -replication_addr=127.0.0.1:6002 \
  -heartbeat_addr=127.0.0.1:9002 \
  -raft_dir=/tmp/gojodb/node2 \
  -controller_addr=127.0.0.1:8081
```

**Node 3:**
```bash
./bin/gojodb_server \
  -node_id=node3 \
  -raft_addr=127.0.0.1:7003 \
  -grpc_addr=127.0.0.1:8003 \
  -http_addr=127.0.0.1:8083 \
  -replication_addr=127.0.0.1:6003 \
  -heartbeat_addr=127.0.0.1:9003 \
  -raft_dir=/tmp/gojodb/node3 \
  -controller_addr=127.0.0.1:8081
```

**Assign a shard:**
```bash
./bin/gojodb_cli
gojodb> admin assign_slot_range 0 16383 node1 node2,node3
gojodb> status
```

### Docker Compose

```bash
docker-compose up --build -d
# Jaeger UI: http://localhost:16686
# Node 1 gRPC: localhost:8001
# Node 2 gRPC: localhost:8002
# Node 3 gRPC: localhost:8003
```

The Compose file starts three storage nodes with persistent volumes under `./data/node{1,2,3}/`.

---

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-node_id` | `node1` | Unique node identifier |
| `-raft_addr` | `127.0.0.1:7000` | Raft TCP bind address |
| `-raft_dir` | `/tmp/gojodb_raft_data` | Raft log & snapshot directory |
| `-grpc_addr` | `127.0.0.1:8000` | gRPC server bind address |
| `-http_addr` | `127.0.0.1:8080` | HTTP server bind address (GraphQL, health) |
| `-replication_addr` | `127.0.0.1:6000` | Log replication listener |
| `-heartbeat_addr` | `127.0.0.1:8081` | Controller heartbeat port |
| `-bootstrap` | `false` | Bootstrap a new Raft cluster (first node only) |
| `-controller_addr` | `127.0.0.1:8080` | Address of bootstrap node for joining cluster |
| `-oltp_endpoint` | `127.0.0.1:4317` | OpenTelemetry collector gRPC endpoint |
| `-log_file` | `/tmp/<node_id>` | Log file path |

---

## Client SDK

**Path**: `pkg/client_sdk/` (work in progress)

Example usage (Go):

```go
import "github.com/sushant-115/gojodb/pkg/client_sdk"

c, err := client_sdk.New("localhost:8000", client_sdk.Options{
    Token: "<jwt>",
})
if err != nil { ... }
defer c.Close()

// Write
err = c.Put(ctx, "mykey", []byte("myvalue"))

// Read
val, found, err := c.Get(ctx, "mykey")

// Range scan
entries, err := c.GetRange(ctx, "a", "z", 100)

// Full-text search
results, err := c.TextSearch(ctx, "content", "hello world")
```

For lower-level access, use the generated gRPC client in `api/proto/`:

```go
conn, _ := grpc.Dial("localhost:8000", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := pb.NewGatewayServiceClient(conn)
resp, err := client.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v")})
```

---

## Query Language (GQL)

> Full reference: [docs/query-language.md](query-language.md)

GojoDB uses a custom query language called **GQL** (not SQL). Statements are sent to the executor via the `core/query_engine` package or through the HTTP/GraphQL API.

### Statements

```sql
-- Point lookup
SELECT * FROM my_index WHERE key = 'user:42'

-- Range scan
SELECT * FROM my_index WHERE key BETWEEN 'user:00' AND 'user:99' ORDER BY key LIMIT 50

-- Full-text search
SELECT * FROM my_index WHERE CONTAINS(body, 'distributed database')

-- Spatial proximity (radius in metres)
SELECT * FROM my_index WHERE NEAR(37.7749, -122.4194, 5000)

-- Spatial bounding box
SELECT * FROM my_index WHERE WITHIN(37.0, -123.0, 38.0, -122.0)

-- Insert
INSERT INTO my_index (key, value) VALUES ('user:42', '{"name":"Alice"}')

-- Delete
DELETE FROM my_index WHERE key = 'user:42'

-- Update
UPDATE my_index SET value = '{"name":"Bob"}' WHERE key = 'user:42'
```

### Predicate operators

| Operator | Example |
|----------|---------|
| `=` | `key = 'x'` |
| `!=` / `<>` | `key != 'x'` |
| `<`, `<=`, `>`, `>=` | `score >= '90'` |
| `BETWEEN … AND …` | `key BETWEEN 'a' AND 'z'` |
| `CONTAINS(field, 'text')` | Full-text search |
| `NEAR(lat, lon, radius)` | Spatial proximity |
| `WITHIN(minLat, minLon, maxLat, maxLon)` | Bounding box |
| `AND`, `OR`, `NOT` | Boolean combinators |

---

## Observability

GojoDB is instrumented with **OpenTelemetry** traces and **Prometheus** metrics.

| Signal | Transport | Default endpoint |
|--------|-----------|-----------------|
| Traces (OTLP) | gRPC | `127.0.0.1:4317` (configurable via `-oltp_endpoint`) |
| Metrics (Prometheus) | HTTP scrape | `:9112/metrics` |
| Trace UI (Jaeger) | HTTP | `http://localhost:16686` (Docker Compose) |

Trace spans are propagated from the Gateway through gRPC middleware (`otelgrpc`) to storage nodes.

---

## Project Layout

```
gojodb/
├── api/                        # API service implementations & protobuf
│   ├── proto/                  # Generated .pb.go + gRPC client helper
│   ├── basic/                  # Simple HTTP+gRPC server
│   ├── indexed_reads_service/  # Get, GetRange, TextSearch handlers
│   ├── indexed_writes_service/ # Put, Delete, BulkPut handlers
│   ├── bulk_writes_service/    # High-throughput write path
│   ├── aggregation_service/    # Aggregation query handlers
│   ├── graphql_service/        # GraphQL server (gqlgen)
│   └── common/                 # Shared auth, error, validation, formatters
├── cmd/
│   ├── gojodb_server/          # Main storage-node binary
│   ├── gojodb_controller/      # Controller-cluster binary
│   ├── gojodb_gateway/         # Shard-routing gateway binary
│   └── gojodb_cli/             # Interactive CLI
├── config/                     # YAML configs + TLS cert helpers
├── core/
│   ├── controller/             # Leader election, node discovery, scheduler
│   ├── indexing/               # B-tree, inverted index, spatial R-tree
│   ├── indexmanager/           # Unified IndexManager interface
│   ├── persistence/            # Persistence helpers (WIP)
│   ├── query_engine/           # Parser, optimizer, executor
│   ├── replication/            # Raft FSM + log replication managers
│   ├── resource_management/    # Admission control, throttling, quotas
│   ├── security/               # JWT auth, RBAC, AES-GCM, KMS envelope
│   ├── sharding/               # Coordinator + metadata store
│   ├── storage_engine/         # Hot/cold/tiered storage adapters
│   ├── transaction/            # 2PC coordinator + lock manager
│   └── write_engine/           # WAL, buffer pool, disk manager
├── data_management/            # Backup, restore, archival, schema migration
├── docs/                       # This documentation
├── examples/                   # Go and Python client examples
├── internal/                   # Shared utilities, telemetry, types
├── pkg/
│   ├── client_sdk/             # Public Go client library
│   ├── common_models/          # Shared model types
│   ├── logger/                 # Zap logger factory
│   ├── lru/                    # Generic LRU cache
│   └── telemetry/              # OpenTelemetry + Prometheus setup
├── tests/                      # Integration & unit tests
├── docker-compose.yml
└── go.mod
```
