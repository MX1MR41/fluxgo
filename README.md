# FluxGo

**A lightweight, Kafka-inspired message broker written in Go.**

FluxGo is a persistent, append-only log broker. Producers append records to topic-partitions, and consumers pull batches of records by offset and track their own progress, optionally persisting it on the broker per consumer group. It deliberately implements a small, coherent subset of Apache Kafka (single node, no replication) and aims to be easy to read, run, and hack on.

It ships as two small binaries (a broker and a CLI client), one third-party dependency (`gopkg.in/yaml.v3`), and a test suite that covers the storage engine, crash recovery, retention, and the wire protocol end to end.

> **Current version: V2.** V2 is a correctness-focused overhaul of the original V1 release: same scope and the same model, but with storage bugs fixed, retention actually enforced, batched fetches, a cleaner protocol, and tests. V2 is **not wire-compatible with V1**. See [DOCUMENTATION.md](DOCUMENTATION.md#19-v1--v2-changelog) for the full changelog.

---

## Table of Contents

- [Why FluxGo](#why-fluxgo)
- [Features](#features)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
- [Building](#building)
- [Running the Broker](#running-the-broker)
- [Using the CLI Client](#using-the-cli-client)
- [Configuration](#configuration)
- [Wire Protocol at a Glance](#wire-protocol-at-a-glance)
- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Data Directory Layout](#data-directory-layout)
- [Durability, Recovery, and Retention](#durability-recovery-and-retention)
- [Delivery Semantics](#delivery-semantics)
- [Development and Testing](#development-and-testing)
- [Limitations and Non-Goals](#limitations-and-non-goals)
- [Roadmap](#roadmap)
- [Contributing](#contributing)

---

## Why FluxGo

FluxGo exists for three reasons:

1. **Learning.** It is a hands-on exercise in building the core of a distributed-systems component: a log-structured message broker. The codebase is intentionally small and readable.
2. **Fidelity to the log model.** It implements the ideas that make Kafka-style brokers work (partitioned, immutable logs, offsets owned by consumers, retention instead of delete-on-read) without the operational weight.
3. **Easy deployment.** The broker is a single statically linked Go binary with a single YAML config file and no external services.

**Goals**

- Implement core Kafka-like messaging primitives.
- Use Go's concurrency model (a goroutine per connection) to serve many clients efficiently.
- Keep I/O efficient and overhead minimal (sequential appends, O(1) index lookups, batched fetches).
- Stay understandable: a codebase you can read in an afternoon.

If you need replication, group rebalancing, security, or transactions, you need Kafka. See [Limitations and Non-Goals](#limitations-and-non-goals).

---

## Features

- **TCP broker** with a simple length-prefixed binary protocol (protocol V2).
- **Topics and partitions** created lazily on first produce. Partitions are numeric IDs (`uint32`); each is an independent log.
- **Segmented, persistent commit log** per partition:
  - Append-only `.log` files plus a per-record `.index` file for O(1) lookups inside a segment.
  - Size-based segment rollover (`max_segment_bytes`).
  - Size-based retention (`max_log_bytes`) enforced after every append; oldest whole segments are deleted.
- **Crash recovery.** The `.log` file is the source of truth: torn record tails are truncated and stale or truncated indexes are rebuilt at startup.
- **Configurable durability.** `file_sync: true` fsyncs the log and index after every append; `false` trades durability for throughput.
- **Produce / Fetch**: fetch is batched (limited by record count and bytes) and returns the high watermark so clients know where the log ends.
- **Explicit offset errors.** Fetch distinguishes *past end* (no new data yet, carries the high watermark) from *out of range* (data deleted by retention, carries the low watermark), so clients can resync cleanly.
- **Consumer-group offsets.** Commit and fetch the next-offset-to-consume per `(group, topic, partition)`, stored atomically on disk.
- **Metadata.** `ListTopics` returns all known topics.
- **Name validation.** Topic and group names are strictly validated (`[a-zA-Z0-9._-]`, max 249 chars), so no name can escape the data directory.
- **Robustness.** Frame size limits, per-request read/write deadlines, per-connection panic recovery, and graceful shutdown.
- **Operability.** Structured logging via `log/slog`, a `-verbose` debug flag, and config validation with clear error messages.
- **CLI client** for producing, fetching (with `-follow`), committing offsets, fetching offsets, and listing topics.

---

## Core Concepts

| Concept | Meaning |
| --- | --- |
| **Broker** | The server process. Accepts client connections, manages logs, and persists consumer offsets. |
| **Topic** | A named feed of records, e.g. `orders`. Names are validated and may contain `_`. |
| **Partition** | A topic is split into numeric partitions (`0`, `1`, …). Each partition is an ordered, append-only log. Ordering is guaranteed only *within* a partition. The client chooses the partition explicitly (default `0`). |
| **Record** | An opaque byte payload (no key, headers, or timestamp). May be empty. |
| **Offset** | A sequential, 0-based 64-bit ID assigned to each record in a partition on append. Consumers address data by offset. |
| **High watermark** | The offset that will be assigned to the *next* record (one past the last stored record; `0` for an empty log). Fetching at or beyond it means "no new data yet". |
| **Low watermark** | The oldest offset still retained. After retention deletes old segments it moves forward. Fetching below it is "out of range". |
| **Segment** | A `.log` + `.index` file pair covering a contiguous range of offsets. A partition is a sequence of segments. |
| **Producer** | A client that appends records to a topic-partition and receives the assigned offset. |
| **Consumer** | A client that reads records sequentially from a partition starting at an offset of its choosing. |
| **Consumer group offset** | A per-`(group, topic, partition)` value stored by the broker: the **next offset the group should consume**. The broker only stores it; it does not coordinate group members. |
| **Retention** | Records are *not* deleted when read. They are removed only when the partition exceeds `max_log_bytes` (oldest whole segments first). This is what allows replay and independent consumers. |

---

## Quick Start

```sh
# 1. Build both binaries (Go 1.23+)
go build -o fluxgo-server ./cmd/fluxgo-server
go build -o fluxgo-client ./cmd/fluxgo-client

# 2. Start the broker (terminal 1)
./fluxgo-server -config configs/server.yaml

# 3. Produce and consume (terminal 2)
./fluxgo-client -action produce -topic orders -message "order #123"
./fluxgo-client -action fetch   -topic orders -offset 0 -count 10
```

Expected client output:

```
produced to orders_0 at offset 0
[offset 0] order #123
```

---

## Building

**Prerequisites:** Go **1.23 or later** ([install](https://go.dev/doc/install)). No other tooling is required.

```sh
# Linux / macOS
go build -o fluxgo-server ./cmd/fluxgo-server
go build -o fluxgo-client ./cmd/fluxgo-client
```

```powershell
# Windows
go build -o fluxgo-server.exe .\cmd\fluxgo-server
go build -o fluxgo-client.exe .\cmd\fluxgo-client
```

To build everything (including a compile check of the tests): `go build ./...`

---

## Running the Broker

```sh
./fluxgo-server -config configs/server.yaml
./fluxgo-server -config configs/server.yaml -verbose   # debug-level logs
```

| Flag | Default | Description |
| --- | --- | --- |
| `-config` | `configs/server.yaml` | Path to the YAML configuration file. The file must exist. |
| `-verbose` | `false` | Enable debug logging (connection open/close, etc.). |

On startup the broker:

1. Loads and validates the configuration (exits with an error on invalid values).
2. Creates the data directory if needed.
3. Opens every existing topic-partition log, **running crash recovery on each**.
4. Opens the consumer-offset store.
5. Starts listening for TCP connections.

Logs are written to stdout in `slog` text format, for example:

```
time=… level=INFO msg="broker listening" address=[::]:9898
```

**Graceful shutdown:** send `SIGINT` (Ctrl+C) or `SIGTERM`. The broker stops accepting connections, closes active connections, waits for in-flight handlers to finish, and then closes all logs.

---

## Using the CLI Client

The client opens one TCP connection, performs one action, and exits (except in `-follow` mode).

```sh
# Produce a record (partition defaults to 0)
./fluxgo-client -action produce -topic orders -message "order #123"

# Produce to a specific partition
./fluxgo-client -action produce -topic orders -partition 2 -message "order #124"

# Fetch up to 10 records starting at offset 0
./fluxgo-client -action fetch -topic orders -offset 0 -count 10

# Follow a partition: keep polling for new records (Ctrl+C to stop)
./fluxgo-client -action fetch -topic orders -offset 0 -count 100 -follow

# Save progress for a consumer group (value = NEXT offset to consume)
./fluxgo-client -action commit -group my-app -topic orders -offset 10

# Read the saved progress back
./fluxgo-client -action fetch-offset -group my-app -topic orders

# List topics
./fluxgo-client -action topics
```

### Client flags

| Flag | Default | Description |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9898` | Broker address. |
| `-action` | `produce` | One of `produce`, `fetch`, `commit`, `fetch-offset`, `topics`. |
| `-topic` | `test-topic` | Topic name. |
| `-partition` | `0` | Partition ID. |
| `-group` | *(empty)* | Consumer group ID. **Required** for `commit` and `fetch-offset`. |
| `-message` | `Hello FluxGo!` | Message body for `produce`. |
| `-offset` | `0` | For `fetch`: starting offset. For `commit`: the offset to store. |
| `-count` | `1` | Maximum records per fetch request. |
| `-max-bytes` | `1048576` (1 MiB) | Maximum bytes per fetch request. |
| `-follow` | `false` | After a fetch, keep polling for new records (500 ms between polls when caught up). |
| `-timeout` | `10s` | Dial/read/write timeout. |

Run `./fluxgo-client -help` for the same list.

### Fetch behavior in the CLI

- Each record prints as `[offset N] <payload>`.
- If you are caught up and `-follow` is **not** set, the client prints `no new data (past end of log)` and exits successfully.
- If the requested offset is older than the retained data, the client prints the earliest available offset. With `-follow` it automatically resumes from there; otherwise it exits with an error.
- `commit` stores the value you give it verbatim. By convention this is **the next offset to read** (`last processed offset + 1`).

---

## Configuration

Configuration lives in a YAML file (default `configs/server.yaml`). Any key you omit falls back to the built-in default shown below. The file itself must exist.

```yaml
# FluxGo broker configuration (V2).
server:
  listen_address: ":9898"   # TCP address the broker listens on
  read_timeout: 60s         # max wait for a complete request frame
  write_timeout: 60s        # max wait writing a response frame
  max_frame_bytes: 16777216 # 16 MiB; bounds a single request/message

log:
  data_dir: ./fluxgo-data       # base directory for all topic-partition logs
  max_segment_bytes: 33554432   # 32 MiB; roll to a new segment past this size
  max_log_bytes: 2147483648     # 2 GiB per partition; oldest segments deleted past this (0 disables)
  file_sync: true               # fsync after every append (durable but slower)
```

| Key | Built-in default | Description |
| --- | --- | --- |
| `server.listen_address` | `127.0.0.1:9898` | TCP bind address. Use `":9898"` to listen on all interfaces. The shipped `server.yaml` does this. |
| `server.read_timeout` | `60s` | Per-request deadline for reading a complete request frame. An idle connection that sends nothing within this window is closed. `0` disables. |
| `server.write_timeout` | `60s` | Deadline for writing a response frame. `0` disables. |
| `server.max_frame_bytes` | `16777216` (16 MiB) | Maximum request frame size, which bounds the largest producible message. Must be between `1024` and `1073741824`. |
| `log.data_dir` | `./fluxgo-data` | Base directory for all data (made absolute at load time). |
| `log.max_segment_bytes` | `33554432` (32 MiB) | A segment is rolled once it reaches this size. A single record may exceed it. Must be `> 0`. |
| `log.max_log_bytes` | `2147483648` (2 GiB) | Per-partition retention limit, counted over `.log` bytes. `0` disables retention. If non-zero, must be `>= max_segment_bytes`. |
| `log.file_sync` | `true` | `true`: fsync log and index after every append (durable, slower). `false`: rely on the OS page cache (faster; recent writes can be lost on power loss or OS crash). |

Invalid configurations are rejected at startup with a descriptive error. See [DOCUMENTATION.md](DOCUMENTATION.md#12-configuration-reference) for the validation rules.

---

## Wire Protocol at a Glance

All communication is over TCP using length-prefixed frames. All integers are **big-endian**.

```
Request : [ 4B length N ][ 1B command code ][ N-1 bytes payload ]
Response: [ 4B length M ][ 1B error code   ][ M-1 bytes payload ]
```

The length covers the code byte plus the payload. Strings are `[u16 length][bytes]`; byte blobs are `[u32 length][bytes]`.

| Code | Command | Request payload | Response payload |
| --- | --- | --- | --- |
| `0x01` | **Produce** | `[u16 topic][u32 partition][u32 len][data]` | `[u64 offset]` |
| `0x02` | **Fetch** | `[u16 topic][u32 partition][u64 offset][u32 maxRecords][u32 maxBytes]` | `[u64 highWatermark][u64 startOffset][u32 count]` then `count × [u32 len][data]` |
| `0x03` | **CommitOffset** | `[u16 group][u16 topic][u32 partition][u64 offset]` | *(empty)* |
| `0x04` | **FetchOffset** | `[u16 group][u16 topic][u32 partition]` | `[u64 offset]` |
| `0x05` | **ListTopics** | *(empty)* | `[u16 count]` then `count × [u16 len][topic]` |

**Error codes** (first byte of every response):

| Code | Name | Meaning |
| --- | --- | --- |
| `0x00` | None | Success. |
| `0x01` | UnknownCommand | Unrecognized command code. |
| `0x02` | MalformedRequest | Payload could not be decoded. |
| `0x03` | MessageTooLarge | Reserved (see [DOCUMENTATION.md](DOCUMENTATION.md#9-wire-protocol-specification)). |
| `0x04` | InvalidTopic | Topic or group name failed validation. |
| `0x05` | TopicNotFound | Topic or partition does not exist (fetch). |
| `0x06` | OffsetOutOfRange | Offset is older than retained data. Payload: `[u64 lowWatermark]`. |
| `0x07` | OffsetPastEnd | No data at that offset yet. Payload: `[u64 highWatermark]`. A normal polling outcome. |
| `0x08` | OffsetNotFound | No committed offset for this group/topic/partition. |
| `0x09` | Internal | Unexpected broker error. |
| `0x0A` | Unavailable | Broker is shutting down. |

The complete byte-level specification, with worked hex examples, is in [DOCUMENTATION.md](DOCUMENTATION.md#9-wire-protocol-specification). Command and error constants live in `internal/protocol/codes.go`.

---

## Architecture Overview

```
 producer / consumer clients
        │   TCP, protocol V2
        ▼
 internal/broker      Server: accept loop, one goroutine per connection
                      Handler: decode → dispatch → encode
        │
        ▼
 internal/store       map["topic_partition"] → *commitlog.Log
 internal/offset      consumer-group offsets (atomic file commits)
        │
        ▼
 internal/commitlog   Log → []*Segment → .log + .index files
        │
        ▼
 Disk
```

| Package | Responsibility |
| --- | --- |
| `cmd/fluxgo-server` | Broker binary: flags, logging setup, wiring, signal handling. |
| `cmd/fluxgo-client` | CLI client binary. |
| `internal/broker` | TCP `Server` (accept loop, connection tracking, graceful shutdown) and `Handler` (request decode, dispatch, response encode). |
| `internal/protocol` | Wire format: frame read/write, command and error codes, `Encoder`/`Decoder` payload helpers shared by broker and client. |
| `internal/store` | Registry of commit logs keyed by `topic_partition`; loads existing logs at startup and creates new ones lazily. |
| `internal/offset` | Persistent consumer-group offsets with atomic commits. |
| `internal/commitlog` | The storage engine: segments, index, crash recovery, retention, batched reads. |
| `internal/config` | YAML loading, defaults, validation. |
| `internal/validate` | Single source of truth for topic and group name rules. |

The flow of a produce request:

```
Client → Server (TCP accept) → Handler.Handle → handleProduce
       → Store.GetOrCreateLog → Log.Append → Segment.Append (.log write, .index write, optional fsync)
       → response frame [offset] → Client
```

For the complete design (component internals, concurrency model, crash recovery, on-disk formats) read [DOCUMENTATION.md](DOCUMENTATION.md).

---

## Project Structure

```
.
├── DOCUMENTATION.md
├── README.md
├── go.mod
├── cmd
│   ├── fluxgo-client
│   │   └── main.go
│   └── fluxgo-server
│       └── main.go
├── configs
│   └── server.yaml
└── internal
    ├── broker
    │   ├── handler.go
    │   ├── integration_test.go
    │   └── server.go
    ├── commitlog
    │   ├── index.go
    │   ├── log.go
    │   ├── log_test.go
    │   └── segment.go
    ├── config
    │   └── config.go
    ├── offset
    │   ├── manager.go
    │   └── manager_test.go
    ├── protocol
    │   ├── codes.go
    │   ├── wire.go
    │   └── wire_test.go
    ├── store
    │   ├── store.go
    │   └── store_test.go
    └── validate
        └── validate.go
```

Module path: `github.com/MX1MR41/fluxgo`. The only external dependency is `gopkg.in/yaml.v3`.

---

## Data Directory Layout

Everything the broker persists lives under `log.data_dir`:

```
fluxgo-data/
├── __consumer_offsets/            consumer-group offsets
│   └── my-app/                    one directory per group
│       └── orders_0.offset        8 bytes, big-endian uint64
├── orders_0/                      topic "orders", partition 0
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   ├── 00000000000000004211.log   next segment (base offset 4211)
│   └── 00000000000000004211.index
└── orders_1/
    └── …
```

- Each partition directory is named `<topic>_<partition>`. The partition is the part after the **last** underscore, so topic names may themselves contain `_`.
- Segment files are named by their zero-padded 20-digit **base offset**, so lexicographic order equals offset order.
- Directories starting with `__` are reserved for broker-internal data (currently `__consumer_offsets`); avoid topic names that begin with `__`.
- `.log` record format: `[8B length][payload]` repeated. `.index` entry format: `[8B relative offset][8B byte position in .log]`, exactly one 16-byte entry per record.

The data directory is runtime state; keep it out of version control (the repository ignores it).

---

## Durability, Recovery, and Retention

**Durability.** With `file_sync: true` (the default) every append is fsynced to both the `.log` and `.index` before the producer receives its offset, and segment rollover fsyncs the old segment before activating the next. With `file_sync: false`, appends are acknowledged once written to the OS page cache, which is faster but can lose recent records on power loss or an OS crash (a process crash alone does not lose them).

**Crash recovery.** On startup each partition's last segment (and *every* segment when `file_sync: false`) is reconciled against its log file, which is the source of truth: a torn trailing record is truncated, a partial trailing index entry is dropped, and an index that disagrees with the log is rebuilt from a scan. A record that reached the `.log` but not the `.index` before a crash is *adopted* rather than lost.

**Retention.** After every append, if the partition's total `.log` size exceeds `max_log_bytes`, the oldest whole segments are closed and deleted until it fits. The active segment is never deleted. Fetching a deleted offset returns `OffsetOutOfRange` with the new low watermark.

Details and edge cases: [DOCUMENTATION.md §7–8](DOCUMENTATION.md#7-durability-and-crash-recovery).

---

## Delivery Semantics

- **Producer → broker:** acknowledged appends are durable according to `file_sync`. If a producer retries after an ambiguous failure (e.g. timeout), the record may be stored twice. There is no deduplication or idempotent producer, so effective semantics are **at-least-once** (with retries) or **at-most-once** (without).
- **Broker → consumer:** consumers pull. To get at-least-once processing, process a batch and *then* commit `lastProcessedOffset + 1`. Committing before processing gives at-most-once.
- **Ordering:** guaranteed within a partition only.
- **Consumer groups:** the broker stores offsets per group but does not coordinate members. If two consumers of the same group read the same partition, they will interfere with each other's commits.

---

## Development and Testing

```sh
go build ./...
go vet ./...
go test ./...            # run the full suite
go test -race ./...      # with the race detector
```

The suite contains 35 test functions:

| Package | Focus |
| --- | --- |
| `internal/commitlog` | Append/read round trips, rollover, reopen-and-continue (regression for the V1 index bug), three crash-recovery scenarios, retention, batched reads, closed-log behavior, concurrent append/read, orphan index files. |
| `internal/broker` | End-to-end TCP tests: produce/fetch across segment rollovers, past-end and unknown-topic errors, offset commit/fetch, list topics, malformed frames not crashing the server, path-traversal rejection. |
| `internal/protocol` | Frame round trips, oversize/truncated frame rejection, encoder/decoder round trips and short reads, error-code string coverage. |
| `internal/store` | Name format/parse round trips, create-and-reload, `GetLog` never creates, name validation, topics/partitions listing, closed store. |
| `internal/offset` | Commit/fetch round trip, persistence across managers, invalid-name rejection. |

The integration tests start a real broker on an ephemeral port (`127.0.0.1:0`) backed by a temporary directory, so they are safe to run anywhere.

---

## Limitations and Non-Goals

These are conscious scope decisions, not oversights:

- **Single node.** No replication, clustering, or fault tolerance.
- **No consumer-group coordination or rebalancing.** Offsets are stored per group, but membership and partition assignment are not managed.
- **No auth, authorization, or TLS.** Run it on a trusted network.
- **No transactions or exactly-once delivery.**
- **Size-based retention only.** No time-based retention and no log compaction.
- **Pull-only, no long polling.** A fetch past the end returns immediately; clients poll.
- **Opaque records.** No keys, headers, timestamps, compression, or per-record checksums. Partition selection is the client's responsibility.
- **One record per Produce request.** Only fetch is batched.
- **Minimal metadata.** `ListTopics` returns topic names only (no partition or broker info on the wire).

---

## Roadmap

Ideas for future development, roughly in order of likelihood:

- Consumer group coordination and rebalancing (membership, exclusive partition assignment).
- Richer metadata API (partitions per topic, watermarks, broker info).
- Batched produce.
- Time-based log retention.
- Performance work such as buffer pooling with `sync.Pool`.
- *(Long term)* Replication and cluster coordination.

---

## Contributing

FluxGo is primarily a personal learning project, so contributions are not actively sought. Feedback, bug reports, and suggestions are welcome via issues.

---

*Built with Go.*
