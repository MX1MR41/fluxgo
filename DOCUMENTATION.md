# FluxGo: Technical Documentation (V2)

This is the complete technical reference for FluxGo V2. It describes the design, every component, the on-disk formats, the wire protocol, the concurrency model, crash-recovery behavior, configuration, and testing. For a quick introduction and usage instructions, start with [README.md](README.md).

**Audience:** developers who want to understand, extend, debug, or write clients for FluxGo.

---

## Table of Contents

1. [Introduction and Scope](#1-introduction-and-scope)
2. [Design Philosophy: The Distributed Log](#2-design-philosophy-the-distributed-log)
3. [System Architecture](#3-system-architecture)
4. [Process Lifecycle](#4-process-lifecycle)
5. [Component Deep Dive](#5-component-deep-dive)
6. [On-Disk Formats](#6-on-disk-formats)
7. [Durability and Crash Recovery](#7-durability-and-crash-recovery)
8. [Retention](#8-retention)
9. [Wire Protocol Specification](#9-wire-protocol-specification)
10. [Request Workflows](#10-request-workflows)
11. [Concurrency Model](#11-concurrency-model)
12. [Configuration Reference](#12-configuration-reference)
13. [Error Handling](#13-error-handling)
14. [Client Guide](#14-client-guide)
15. [Logging and Observability](#15-logging-and-observability)
16. [Testing](#16-testing)
17. [Limitations and Non-Goals](#17-limitations-and-non-goals)
18. [Future Directions](#18-future-directions)
19. [V1 → V2 Changelog](#19-v1--v2-changelog)
20. [Glossary](#20-glossary)

---

## 1. Introduction and Scope

FluxGo is a single-node, log-structured message broker written in Go. It implements a small subset of Apache Kafka:

- named **topics**, split into numeric **partitions**;
- each partition backed by a persistent, append-only **commit log**;
- **producers** append records and receive back the assigned **offset**;
- **consumers** pull batches of records by offset;
- consumers may persist their progress on the broker as **consumer-group offsets**.

Its primary objectives are to provide these core primitives with good performance, to stay simple enough to read end to end, and to lean on Go's idioms (goroutines, `sync` primitives, explicit errors, the standard library).

**Scope.** FluxGo V2 is a correctness-focused overhaul of V1: the same scope and the same model, but with the storage bugs fixed, retention wired up, batched fetches, a cleaner protocol, and a real test suite. What V1 had, V2 has, only more correct; what V1 deliberately left out (replication, group coordination, security, transactions) remains out. See [§17](#17-limitations-and-non-goals) and [§19](#19-v1--v2-changelog).

**Technology.** Go 1.23+, standard library only, plus `gopkg.in/yaml.v3` for configuration. Module path: `github.com/MX1MR41/fluxgo`.

---

## 2. Design Philosophy: The Distributed Log

FluxGo adopts the **distributed log** abstraction as its core model, as Kafka does. This differs significantly from traditional point-to-point message queues.

- **Append-only log.** Each partition is an ordered, immutable sequence of records appended to segment files on disk.
- **Offsets.** Every record in a partition gets a unique, sequential 64-bit offset on append. The offset is the consumer's coordinate.
- **Consumer responsibility.** Consumers decide where to read from and track how far they have processed. The broker keeps no per-message delivery state. It can optionally store a single "next offset" per consumer group, but it is only storage.
- **Retention, not deletion on read.** Reading never removes data. Records disappear only when retention deletes old segments (currently when a partition exceeds a size limit). Multiple independent consumers can therefore read the same data, and data can be replayed by resetting an offset.
- **Publish/subscribe.** The model naturally supports pub/sub: producers publish to topics and any number of consumers read at their own pace.

### Why this model?

| Benefit | Explanation |
| --- | --- |
| **High throughput** | Appending to the end of a file is sequential I/O, far cheaper than random writes. Sequential reads benefit from OS read-ahead and the page cache. |
| **Decoupling and replay** | The broker does not track per-message acknowledgement. New consumers can join and read history (within retention). Reprocessing is just a matter of resetting an offset. |
| **Scalability path** | Partitioning spreads a topic across independent logs, which lets load be parallelized (and, in future versions, distributed across brokers). |
| **Simplicity** | A log plus an index is a small amount of state to reason about, test, and recover. |

### Go idioms FluxGo leans on

- **Goroutine-per-connection** for cheap concurrency across many clients.
- **`sync.RWMutex`** for many concurrent readers with exclusive writers.
- **Positional I/O** (`ReadAt`/`WriteAt`) to avoid seek races and shared file cursors.
- **`encoding/binary`** for compact, fast serialization.
- **Explicit error values** (`errors.Is`, `%w` wrapping, `errors.Join`) for I/O failures and logical conditions.
- **`context` and `sync.WaitGroup`** for graceful shutdown.
- **`log/slog`** for structured logging.

---

## 3. System Architecture

### 3.1 Layers

```
 producer / consumer clients
        │   TCP, protocol V2
        ▼
 ┌──────────────────────────────────────────────────────────┐
 │ internal/broker                                          │
 │   Server : listener, accept loop, connection tracking,   │
 │            one goroutine per connection, shutdown        │
 │   Handler: read frame → decode → dispatch → encode →     │
 │            write frame                                   │
 └──────────────┬──────────────────────────┬────────────────┘
                │                          │
                ▼                          ▼
 ┌──────────────────────────┐   ┌───────────────────────────┐
 │ internal/store           │   │ internal/offset           │
 │  map["topic_partition"]  │   │  consumer-group offsets   │
 │   → *commitlog.Log       │   │  (atomic file commits)    │
 └──────────────┬───────────┘   └─────────────┬─────────────┘
                │                             │
                ▼                             │
 ┌──────────────────────────┐                 │
 │ internal/commitlog       │                 │
 │  Log → []*Segment        │                 │
 │  Segment → .log + .index │                 │
 └──────────────┬───────────┘                 │
                ▼                             ▼
              Disk  (<data_dir>/<topic>_<partition>/…   <data_dir>/__consumer_offsets/…)
```

### 3.2 Packages

| Package | Responsibility |
| --- | --- |
| `cmd/fluxgo-server` | Broker binary: flag parsing, logger setup, component wiring, signal handling. |
| `cmd/fluxgo-client` | CLI client: produce, fetch (`-follow`), commit, fetch-offset, topics. |
| `internal/broker` | `Server` (TCP lifecycle) and `Handler` (per-connection request processing). |
| `internal/protocol` | Frame I/O, command/error codes, field-width constants, `Encoder`/`Decoder`. Shared by broker and client. |
| `internal/store` | Registry of commit logs keyed by `topic_partition`; name formatting/parsing; lazy creation; startup loading. |
| `internal/offset` | Persistent consumer-group offset storage. |
| `internal/commitlog` | Storage engine: `Log`, `Segment`, `index`, recovery, retention, batched reads. |
| `internal/config` | YAML configuration: defaults, loading, validation, derivation of per-log config. |
| `internal/validate` | The single definition of what a valid topic or group name is. |

### 3.3 Dependency direction

```
cmd/* ─► broker ─► store ─► commitlog
           │         │          ▲
           │         └─► config ┘     (config imports commitlog.Config only)
           ├─► offset ─► validate
           ├─► protocol
           └─► validate
store ─► validate
```

`protocol` and `validate` are leaf packages. `commitlog` knows nothing about networking, topics, or configuration files. It is a self-contained, embeddable storage engine for one partition.

### 3.4 Key design decisions

| Decision | Rationale |
| --- | --- |
| **The `.log` file is the source of truth; the index is rebuildable.** | Recovery never has to trust a possibly-stale index; it can always reconstruct it from the log. |
| **One 16-byte index entry per record (dense index).** | Lookup is arithmetic (`relativeOffset × 16`), O(1), with no binary search inside a segment. The trade-off is a larger index than a sparse one. |
| **Index written with `WriteAt` at a tracked size, not `O_APPEND`.** | Positional writes stay correct across restarts and make out-of-order entries detectable. |
| **Cross-segment lookup by binary search over base offsets.** | O(log segments) with a simple sorted slice. |
| **Shared name validation (`internal/validate`).** | Anything that becomes a path on disk passes through one function, so no name can escape the data directory. |
| **Payloads built with `Encoder`/`Decoder`.** | Sticky-error decoding removes hand-rolled cursor arithmetic and its bounds bugs. |
| **Distinct *past-end* and *out-of-range* errors that carry watermarks.** | Clients can tell "poll again later" from "data was deleted; resync from here" without extra round trips. |
| **Retention in whole segments; active segment never deleted.** | Deleting files is cheap and never races with appends to the active segment. |
| **Per-connection panic recovery.** | A bug triggered by one client's request drops that connection, not the broker. |

---

## 4. Process Lifecycle

### 4.1 Startup (`cmd/fluxgo-server`)

1. Parse flags (`-config`, `-verbose`) and install a `slog` text logger on stdout (level `Info`, or `Debug` with `-verbose`).
2. `config.LoadConfig(path)`: apply defaults, read and unmarshal the YAML file, make `data_dir` absolute, validate. Any failure prints `error: …` to stderr and exits with status 1.
3. `config.EnsureDataDir()`: create the data directory (`0o755`).
4. Log the effective configuration (`listenAddress`, `dataDir`, `maxSegmentBytes`, `maxLogBytes`, `fileSync`).
5. `store.NewStore(...)`: scan the data directory; for each valid `topic_partition` directory, open the log (**running crash recovery**) and register it. Directories whose names start with `__` are skipped; directories with unparseable names are skipped with a warning. A log that fails to open aborts startup.
6. `offset.NewManager(...)`: create `<data_dir>/__consumer_offsets` if needed.
7. `broker.NewServer(...)`: wire the handler.
8. `signal.NotifyContext` on `SIGINT` and `SIGTERM`, then `srv.Start(ctx)`.

### 4.2 Shutdown

When the context is cancelled (or `Server.Stop` is called):

1. The `quit` channel is closed (idempotently, via `sync.Once`).
2. The listener is closed, so the accept loop exits.
3. Every tracked active connection is closed, which unblocks handlers stuck in a read or write.
4. `Stop` waits on the `WaitGroup` until the accept loop and **all** handler goroutines have returned, so any in-flight append finishes first.
5. `Start` returns `nil`; the process logs `shutdown complete`, and the deferred `store.Close()` then closes every log (syncing and closing segment files).

Because the store is closed only after all handlers have exited, no request can be mid-flight against a closing log during a normal shutdown.

### 4.3 Client process (`cmd/fluxgo-client`)

Dials the broker (with `-timeout`), performs exactly one action, and exits, with the exception of `fetch -follow`, which loops until interrupted. Exit codes: `0` success; `1` connection failure, broker error, or malformed response; `2` invalid `-action`.

---

## 5. Component Deep Dive

### 5.1 `internal/commitlog`: the storage engine

Responsible for durable, ordered storage of records for **one partition**. It has no knowledge of topics, networking, or configuration files.

#### Public surface

| Item | Description |
| --- | --- |
| `type Record []byte` | One message payload. |
| `type Config` | `MaxSegmentBytes`, `MaxLogBytes`, `FileSync`. |
| `DefaultConfig()` | Library defaults (16 MiB segments, 1 GiB log, sync on). The broker does not use this; it derives its config from `ServerConfig` (§12). |
| `Open(dir, name, config, logger)` | Open or create a log, running recovery on the segments found. |
| `(*Log).Append(Record) (uint64, error)` | Append and return the absolute offset. Runs retention afterward. |
| `(*Log).Read(offset)` | Convenience wrapper over `ReadBatch(offset, 1, 0)`. |
| `(*Log).ReadBatch(offset, maxRecords, maxBytes)` | Read consecutive records, crossing segment boundaries. |
| `(*Log).HighestOffset()` | High watermark: the offset the next append will receive. |
| `(*Log).LowestOffset()` | Low watermark: the oldest retained offset. |
| `(*Log).Size()` / `SegmentCount()` / `Name()` / `Dir()` | Introspection. |
| `(*Log).Close()` | Seal and close all segments. Idempotent. |

#### Sentinel errors

| Error | Meaning |
| --- | --- |
| `ErrOffsetNotFound` | The offset is not in this segment/log. |
| `ErrOffsetOutOfRange` | The offset is older than the earliest retained record. |
| `ErrReadPastEnd` | The offset is at or beyond the next offset to be assigned (no new data yet). |
| `ErrLogClosed` | Operation on a closed log. |
| `ErrIndexNotFound` | Index entry missing or beyond the index size. |
| `ErrCorruptSegment` | A record's length prefix runs past the end of the log file. |

#### `Log`

A `Log` owns a sorted slice of `*Segment` and a pointer to the **active segment** (always the last one) that receives writes.

- **`Open` / `loadSegments`.** Creates the directory, then lists it. Segment discovery is driven by `.log` files: each file's base offset is parsed from its name (`<20-digit base>.log`); files with unparseable names are ignored with a warning, and an `.index` file without a matching `.log` is ignored entirely. Base offsets are sorted and each segment is opened. A segment is **reconciled** (crash-recovered by scanning its log) if it is the last segment, or if `file_sync` is `false`. If no segments exist, an initial segment with base offset `0` is created. `totalSize` is the sum of the segments' `.log` sizes.
- **`Append`.** Takes the write lock; fails with `ErrLogClosed` if closed. If the active segment `IsFull()` (its size is at or above `MaxSegmentBytes`), it rolls first. It then appends to the active segment, adds `len(record) + 8` to `totalSize`, and runs retention. Retention errors are logged but do **not** fail the append, because the record is already durably stored.
- **Rollover (`rollSegmentLocked`).** Syncs the old segment's index and log (failures are logged as warnings), opens a new segment whose base offset is the old segment's `NextOffset()`, appends it to the slice, and makes it active.
- **`ReadBatch`.** Takes the read lock and validates the offset against the watermarks, read **directly** from the segments rather than through the locking accessors (see §11): `offset < low` returns `ErrOffsetOutOfRange`; `offset >= high` returns `ErrReadPastEnd`. It then locates the starting segment with a binary search (`sort.Search` on base offsets) and reads from each segment in turn until `maxRecords` records are collected or the byte budget is spent. See the batch semantics below.
- **`applyRetentionLocked`.** See [§8](#8-retention).
- **`Close`.** Marks the log closed, closes every segment, and clears state. Subsequent operations return `ErrLogClosed`.

**Batch read semantics**

- `maxRecords` is clamped to at least 1.
- `maxBytes` bounds the total **on-disk size** of the returned records, counting `payload + 8` bytes (the length prefix) per record. `0` means no limit.
- The **first** record is always returned even if it alone exceeds `maxBytes`, so a consumer can always make progress. After the first record, the limit is strict across all segments.
- A batch continues across segment boundaries transparently.
- If no records could be returned, `ErrReadPastEnd` is returned.

#### `Segment`

One `.log` / `.index` pair covering offsets `[baseOffset, nextOffset)`.

- **State:** directory, `baseOffset`, `nextOffset`, `maxBytes`, the two file handles, `storeSize` (current `.log` size), and the `fileSync` flag, all protected by an `RWMutex`.
- **Open (`openSegment`).** Opens the `.log` with `O_RDWR|O_CREATE|O_APPEND` and the `.index` with `O_RDWR|O_CREATE` (**deliberately without `O_APPEND`**, because index entries are written positionally). Records the log's size, then runs `recover`.
- **`Append`.** Under the write lock: assigns `offset = nextOffset` and `position = storeSize`; builds a single buffer `[8B length][payload]` and writes it with one `Write` call; writes the matching index entry; if `fileSync`, fsyncs the log and then the index; finally increments `nextOffset`. The assigned absolute offset is returned.
- **`readBatch`.** Looks up the byte position of the first requested record via the index (O(1)), then reads records **sequentially** from the `.log` using `ReadAt` (read the 8-byte length, then the payload), stopping at the end of the segment, at `maxRecords`, or at the byte limit. A record whose length runs past the end of the file yields `ErrCorruptSegment`.
- **`IsFull`.** `maxBytes > 0 && storeSize >= maxBytes`. Because the check happens *before* the next append, a segment can end up slightly larger than `max_segment_bytes` (by at most one record), and a single record larger than the limit is accepted into its own segment.
- **`Close` / `Remove`.** `Close` closes the index, then syncs and closes the log. `Remove` deletes both files (the segment must be closed first); missing files are not an error.

#### `index`

A thin wrapper over the `.index` file.

- Fixed entry width of **16 bytes**; the entry for relative offset *N* lives at byte `N × 16`.
- `ReadPositionForOffset(rel)`: bounds-checks against the tracked size and reads the entry with `ReadAt`; returns the byte position in the `.log` (the second 8 bytes).
- `WriteEntry(rel, pos)`: requires `rel` to equal the next expected entry (`size / 16`); an out-of-order write is reported as an error rather than corrupting the file. Written with `WriteAt` at the tracked size.
- `rewrite(positions)`: truncates the file and writes a fresh index from a list of positions, then fsyncs (used by recovery).
- `truncateToWholeEntries()`: drops a torn trailing entry.
- Has its own `RWMutex`.

---

### 5.2 `internal/store`: log registry

Manages the collection of `*commitlog.Log` instances for all topic-partitions and is the handler's entry point to the storage layer.

- **State:** base directory, `*config.ServerConfig`, logger, `map[string]*commitlog.Log` keyed by log name (`"orders_0"`), a `closed` flag, and an `RWMutex`.
- **Naming.** `FormatLogName(topic, partition)` yields `topic_partition`. `ParseLogName` splits at the **last** underscore, requires a numeric (`uint32`) partition and a non-empty topic, and validates the topic via `internal/validate`. Topics may therefore contain underscores (`my_topic_3` → topic `my_topic`, partition `3`).
- **Startup load (`loadExistingLogs`).** As described in §4.1: skips non-directories, skips `__`-prefixed directories, warns about and skips invalid names, opens the rest, and logs each log's low/high watermark.
- **`GetLog(topic, partition)`.** Read-only lookup under a read lock; returns `nil` if the log does not exist or the store is closed. It **never creates anything** (used by Fetch).
- **`GetOrCreateLog(topic, partition)`.** Validates the topic name first; then does a **double-checked lookup**: check under the read lock; if missing, take the write lock, check again (another goroutine may have created it), and only then call `commitlog.Open`. Returns `ErrStoreClosed` after `Close`. (Used by Produce.)
- **`Topics()`.** Sorted, de-duplicated topic names derived from the open logs.
- **`Partitions(topic)`.** Sorted partition IDs for a topic (available internally; not exposed on the wire in V2).
- **`Close()`.** Closes every log, collecting errors with `errors.Join`; idempotent.

---

### 5.3 `internal/offset`: consumer-group offset persistence

Stores, per `(group, topic, partition)`, the **next offset the group should consume**, so consumers can resume after restarts. The broker only stores the value; it does not interpret or validate it.

- **Layout:** `<data_dir>/__consumer_offsets/<group>/<topic>_<partition>.offset`; each file holds exactly 8 bytes, a big-endian `uint64`.
- **`Commit`.** Validates group and topic (`internal/validate`), then under the write lock: creates the group directory, writes the 8 bytes to `<file>.tmp`, `fsync`s it, closes it, atomically `rename`s it over the target, and finally makes a best-effort `fsync` of the directory so the rename itself survives a crash. On any failure the temp file is removed. A reader therefore sees either the old value or the new value, never a partial one.
- **`Fetch`.** Validates names, then under the read lock reads the 8 bytes. A missing file yields `ErrOffsetNotFound`; a short file yields a "corrupt" error.
- **Concurrency.** A single `RWMutex` guards all groups: commits are serialized with each other, fetches run concurrently.

---

### 5.4 `internal/protocol`: wire format

Defines the contract shared by broker and clients.

- **Constants:** field widths (`LenPrefixSize=4`, `CodeSize=1`, `TopicLenSize=2`, `GroupIDLenSize=2`, `PartitionIDSize=4`, `OffsetSize=8`, `CountSize=4`, `RecordLenSize=4`), `DefaultMaxFrameBytes` (16 MiB, the client's fallback), command codes (`CmdProduce` … `CmdListTopics`), error codes (`ErrCodeNone` … `ErrCodeUnavailable`), and `ErrorCodeToString`.
- **`WriteFrame(w, code, payload)`.** Builds `[4B len][1B code][payload]` in a single buffer and writes it with one `Write`.
- **`ReadFrame(r, maxFrameBytes)`.** Reads the 4-byte length with `io.ReadFull`; rejects lengths below 1 and above `maxFrameBytes` (a `maxFrameBytes` of `0` means no limit); reads the body; returns the code byte and payload.
- **`Encoder`.** Append-only builder: `Uint8/16/32/64`, `String` (`u16` length prefix), `Bytes` (`u32` length prefix), `Payload`, `Len`.
- **`Decoder`.** Sequential reader with a **sticky error**: after the first short read every later call returns a zero value, so a handler can decode a whole struct and check `Err()` once. `String`/`Bytes` check lengths against the remaining buffer. `Bytes` returns a slice that *aliases* the decoder's buffer. `Remaining()` reports undecoded bytes. Handlers do not reject trailing bytes.

---

### 5.5 `internal/broker`: network interaction and request orchestration

#### `Server`

- **State:** config, logger, the `Handler`, the `net.Listener`, a `quit` channel with a `sync.Once`, a `sync.WaitGroup`, and a mutex-guarded map of active connections.
- **`Start(ctx)`.** `net.Listen("tcp", listen_address)`, starts `acceptLoop` in a goroutine, then blocks until `ctx` is done or `quit` is closed, then calls `Stop()` and returns `nil`. A failure to listen is returned as an error.
- **`Addr()`.** The listener's address, or `nil` before `Start` has bound it (useful with port `0` in tests).
- **`acceptLoop`.** Loops on `Accept`. On error it exits if the server is quitting or the listener is closed, and otherwise logs a warning and continues. For each connection it registers it in `activeConns`, increments the `WaitGroup`, and launches a goroutine that runs `handler.Handle(conn)` and, on exit, unregisters and closes the connection.
- **`Stop()`.** As described in §4.2. Safe to call multiple times.

#### `Handler`

Serves one connection. Holds the store, the offset manager, a logger, `maxFrameBytes`, and the read/write timeouts.

**Processing loop (`Handle`)**, run per connection:

1. Set the read deadline (`now + read_timeout`, if non-zero).
2. Read one request frame with `proto.ReadFrame` over a `bufio.Reader`, enforcing `max_frame_bytes`. On EOF, closed connection, or timeout, return silently; on other read errors (including oversized or malformed frames), log a warning and return. **The connection is then closed with no response.**
3. `dispatch` on the command code to a `handle*` function, which returns `(errorCode, payload)`.
4. Set the write deadline and write the response frame. A write failure ends the connection.
5. Repeat.

A `defer`red `recover` wraps the whole loop: a panic is logged and only that connection is dropped.

Requests on a connection are processed strictly one at a time, in order. The read deadline is reset per request, so `read_timeout` also acts as an **idle timeout**.

**Hard caps on Fetch**, regardless of what the client asks for: at most **4096** records and **32 MiB** per response.

The individual handlers are specified in [§9](#9-wire-protocol-specification) and [§10](#10-request-workflows).

---

### 5.6 `internal/config`: configuration

- **`ServerConfig`** holds `ServerSettings` (`listen_address`, `read_timeout`, `write_timeout`, `max_frame_bytes`) and `LogSettings` (`data_dir`, `max_segment_bytes`, `max_log_bytes`, `file_sync`), mapped to YAML via struct tags.
- **`LoadConfig(path)`.** Starts from built-in defaults, reads the file (a missing file is an error), unmarshals over the defaults with `gopkg.in/yaml.v3` (so omitted keys keep their defaults), makes `data_dir` absolute, and validates.
- **`Validate()`.** Rules are listed in [§12](#12-configuration-reference).
- **`EnsureDataDir()`.** Creates the data directory.
- **`GetCommitLogConfig()`.** Bridges the global `LogSettings` to the per-log `commitlog.Config` used whenever the store opens a log.

### 5.7 `internal/validate`: name rules

A single check applied to topic names and group IDs, since both become file or directory names:

- must not be empty;
- at most **249** characters (mirroring Kafka's topic limit);
- must not be `.` or `..`;
- only the characters `a-z A-Z 0-9 . - _`.

Rejections return an error wrapping `validate.ErrInvalidName`, which the handler maps to `ErrCodeInvalidTopic`. Because path separators are not in the allowed set, a name can never traverse out of the data directory.

> Names starting with `__` pass validation but are treated as broker-internal by the startup loader (which skips such directories). Avoid them.

### 5.8 Binaries

- **`fluxgo-server`:** see [§4.1](#41-startup-cmdfluxgo-server).
- **`fluxgo-client`:** a thin CLI over `protocol`. `roundTrip` applies a deadline to each request/response, and if the broker returns a non-zero error code it returns an error that includes the code, its description, and the message payload while still returning the code so callers can special-case it (`OffsetPastEnd`, `OffsetOutOfRange`, `OffsetNotFound`). See [README.md](README.md#using-the-cli-client) for flags.

---

## 6. On-Disk Formats

All multi-byte integers are **big-endian**.

### 6.1 Data directory

```
<data_dir>/
├── __consumer_offsets/
│   └── <group>/
│       └── <topic>_<partition>.offset         (+ transient .offset.tmp during a commit)
├── <topic>_<partition>/
│   ├── <20-digit base offset>.log
│   ├── <20-digit base offset>.index
│   └── …
└── …
```

- Partition directory: `<topic>_<partition>`; partition is a decimal `uint32` after the last `_`.
- Segment file names are the segment's base offset, zero-padded to **20 digits** (`%020d`), so lexicographic order equals offset order. A segment's `.log` and `.index` share the same stem.

### 6.2 `.log` file

A sequence of records with no header or footer:

```
┌──────────────────┬──────────────────────┐ ┌──────────────────┬───────────┐
│ 8B length N      │ N bytes payload      │ │ 8B length N'     │ …         │
└──────────────────┴──────────────────────┘ └──────────────────┴───────────┘
 ▲ position p0                                ▲ position p1 = p0 + 8 + N
```

There are no per-record checksums, timestamps, or keys. An empty payload (`N = 0`) is valid.

### 6.3 `.index` file

A sequence of fixed 16-byte entries, **one per record**:

```
┌────────────────────────┬────────────────────────┐
│ 8B relative offset     │ 8B position in .log    │
└────────────────────────┴────────────────────────┘
```

- *Relative offset* = absolute offset − segment base offset; entries are strictly sequential (0, 1, 2, …).
- *Position* = byte position of the record's **length prefix** in the `.log` file.
- The entry for relative offset *N* is at byte `N × 16`, so lookup is direct.

### 6.4 `.offset` file

Exactly 8 bytes: the committed `uint64` (the next offset the group should consume).

### 6.5 Worked example

After producing `"hi"` and `"yo"` as the first two records of a fresh partition:

```
00000000000000000000.log   (20 bytes)
  00 00 00 00 00 00 00 02  68 69              ← record 0 at position 0   ("hi")
  00 00 00 00 00 00 00 02  79 6F              ← record 1 at position 10  ("yo")

00000000000000000000.index (32 bytes)
  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00   ← offset 0 → position 0
  00 00 00 00 00 00 00 01  00 00 00 00 00 00 00 0A   ← offset 1 → position 10
```

---

## 7. Durability and Crash Recovery

### 7.1 Write path ordering

For each append, a segment:

1. writes `[length][payload]` to the `.log` in a **single** `Write` call;
2. writes the index entry with `WriteAt` at the tracked index size;
3. if `file_sync: true`, fsyncs the `.log`, then the `.index`;
4. only then advances `nextOffset`, after which the offset is returned to the producer.

The `.log` is opened with `O_APPEND`. The `.index` deliberately is **not**, because positional writes at the tracked size are what keep index appends correct after a restart.

**Segment rollover** fsyncs the old segment's index and log before activating the new one.

### 7.2 `file_sync` modes

| `file_sync` | Behavior | Durability | Throughput |
| --- | --- | --- | --- |
| `true` (default) | fsync log and index after every append. | An acknowledged record survives power loss or OS crash. | Lower. |
| `false` | Writes go to the OS page cache; no per-append fsync (rollover and close still sync). | Survives a process crash; recent records can be lost on power loss or OS crash. | Higher. |

### 7.3 Recovery on open: the log is the source of truth

When a segment is opened, it reconciles its two files. First, a **partial trailing index entry** (a size not divisible by 16) is dropped. Then:

- **Reconciled segments**: the *last* segment always, and *every* segment when `file_sync: false`:
  1. **Scan** the `.log`: walk length prefixes from position 0, recording the start position of every *complete* record. Scanning stops at the first record whose length would run past the end of the file.
  2. **Truncate** the `.log` to the end of the last complete record (removing a torn tail) and fsync it.
  3. **Compare** with the index: if the number of index entries differs from the number of records found, or the last index entry's position disagrees with the scan, **rewrite the whole index** from the scan (and fsync it).
  4. Set `nextOffset = baseOffset + recordsFound`.
- **Sealed segments with `file_sync: true`** (all but the last) are **trusted**: `nextOffset = baseOffset + indexEntries`, with one exception: an *empty* index over a *non-empty* log triggers a full rebuild.

**Why sealed segments can be trusted under `file_sync: true`:** a segment becomes sealed only by rollover, and rollover fsyncs it first; every append to it was also fsynced. A crash therefore cannot leave a sealed segment half-written.

### 7.4 Crash scenarios

| Scenario | On-disk state after the crash | Outcome on restart |
| --- | --- | --- |
| Crash **between the log write and the index write** | A complete record in the `.log` with no index entry. | Scan finds one more record than the index has → index rebuilt → the record is **adopted** rather than silently discarded. (The producer never received an ack and may retry, so the record could appear twice. See [§17](#17-limitations-and-non-goals).) |
| Crash **mid-record** (torn payload or length prefix) | A partial record at the end of the `.log`. | Scan stops before it → `.log` truncated to the last complete record. |
| **Truncated or partial index** | Fewer index entries than records, or a torn entry. | Torn entry dropped; count mismatch → index rebuilt from the log. No data lost as long as the log is intact. |
| **Index longer than the log, or stale** | More index entries than complete records, or a mismatched last position. | Index rewritten from the scan. |
| **Lost index** (empty index, non-empty log) | `.index` empty or missing. | Rebuilt (last segment always; sealed segments via the empty-index exception). |
| **Orphan `.index`** with no `.log` | A stray index file. | Ignored by the loader (it is not turned into a segment). |
| Crash **during rollover** | Old segment fsynced; new segment possibly empty. | New (empty) segment is the last segment; appends continue from its base offset. |

### 7.5 What recovery cannot do

- There are **no checksums**. Recovery detects torn tails by *length*, not content. A bit-flip inside a payload is not detected. A corrupted length prefix in the middle of a reconciled segment is indistinguishable from a torn tail: recovery keeps everything before it and discards the rest of that segment.
- Sealed segments under `file_sync: true` are trusted, not verified.
- The consumer-offset files are protected by atomic rename, not by checksums; a file that is not 8 bytes is reported as corrupt on fetch.

---

## 8. Retention

Retention is **size-based** and enforced **after every successful append** (V1 defined the function but never called it).

**Algorithm (`applyRetentionLocked`, under the log's write lock):**

1. Do nothing if `max_log_bytes <= 0`, if `totalSize <= max_log_bytes`, or if there is only one segment.
2. Walk the segments from oldest to newest, **excluding the active segment**, marking segments for removal while `totalSize − freed > max_log_bytes`.
3. Drop the marked segments from the in-memory slice (the new first segment's base offset becomes the **low watermark**), update `totalSize`, and log an `INFO` message with the number of segments and bytes freed.
4. Close and delete each removed segment's `.log` and `.index`. Failures are collected and returned, but are logged by `Append` without failing the produce.

**Properties**

- Deletion is by **whole segment**, so retention granularity equals `max_segment_bytes`. Actual disk usage can exceed `max_log_bytes` by up to roughly one active segment, because the active segment is never deleted. This is why validation requires `max_log_bytes >= max_segment_bytes`.
- `totalSize` counts `.log` bytes (payload + 8-byte prefix per record) and **excludes `.index` files**.
- The effect on consumers: fetching an offset below the new low watermark returns `ErrCodeOffsetOutOfRange` with the low watermark in the payload, so the consumer can resync (§14).
- Time-based retention and compaction are not implemented.

---

## 9. Wire Protocol Specification

### 9.1 Transport and framing

TCP. Each message in either direction is one **frame**; all integers are big-endian.

```
Request : [ 4B length N ][ 1B command code ][ N-1 bytes payload ]
Response: [ 4B length M ][ 1B error code   ][ M-1 bytes payload ]
```

- The length covers the code byte plus the payload (so the minimum is 1).
- A frame whose length is `0` or exceeds `server.max_frame_bytes` is a protocol violation: the broker logs it and **closes the connection without a response**.
- Within a connection, requests are handled one at a time and responses arrive **in request order**. The protocol has no correlation IDs, so clients should keep a single request in flight per connection (or match responses strictly by order).
- A connection idle longer than `server.read_timeout` is closed by the broker.

### 9.2 Primitive types

| Type | Encoding |
| --- | --- |
| `u8` / `u16` / `u32` / `u64` | Big-endian unsigned integers. |
| `string` | `[u16 length][UTF-8 bytes]` (names must be ≤ 249 bytes in practice). |
| `bytes` | `[u32 length][raw bytes]`. |

Trailing unread bytes at the end of a payload are ignored by the broker.

### 9.3 Command codes

| Code | Name | Purpose |
| --- | --- | --- |
| `0x01` | `Produce` | Append one record; returns its offset. |
| `0x02` | `Fetch` | Read a batch of records starting at an offset. |
| `0x03` | `CommitOffset` | Persist a consumer-group offset. |
| `0x04` | `FetchOffset` | Read back a consumer-group offset. |
| `0x05` | `ListTopics` | Metadata: list known topics. |

### 9.4 Error codes

On success the code is `0x00`. On failure the payload is **either** a UTF-8 diagnostic message **or** a documented binary value, as noted.

| Code | Name | Meaning | Payload |
| --- | --- | --- | --- |
| `0x00` | `None` | Success. | Command-specific. |
| `0x01` | `UnknownCommand` | Unrecognized command code. | Text message. |
| `0x02` | `MalformedRequest` | Payload could not be decoded (too short, bad lengths). | Text message. |
| `0x03` | `MessageTooLarge` | **Reserved.** Defined in the protocol but not currently returned; an oversized request frame closes the connection (§9.1). | n/a |
| `0x04` | `InvalidTopic` | A topic or group name failed validation. | Text message. |
| `0x05` | `TopicNotFound` | Fetch: the topic or partition does not exist. | Text message. |
| `0x06` | `OffsetOutOfRange` | Fetch: offset is older than the retained data (deleted by retention). | `[u64 lowWatermark]` |
| `0x07` | `OffsetPastEnd` | Fetch: offset is at or beyond the next offset to be assigned; no new data yet. A normal polling outcome. | `[u64 highWatermark]` |
| `0x08` | `OffsetNotFound` | FetchOffset: no committed offset for this group/topic/partition. | empty |
| `0x09` | `Internal` | Unexpected broker-side failure. | Text message |
| `0x0A` | `Unavailable` | The log was closed (broker shutting down); currently returned by Fetch. | Text message |

### 9.5 Commands

#### `0x01` Produce

Appends a single record to `topic`/`partition`, creating the topic-partition on first use.

```
Request : [string topic][u32 partition][bytes data]
Response: [u64 offset]                     (the offset assigned to the record)
```

- The topic name is validated (`InvalidTopic` on failure).
- An empty `data` is allowed.
- The maximum record size is bounded by `server.max_frame_bytes` less the few bytes of frame and field headers.
- Errors: `MalformedRequest`, `InvalidTopic`, `Internal` (store closed, log open failure, or append failure).

#### `0x02` Fetch

Reads up to `maxRecords` consecutive records starting at exactly `offset`.

```
Request : [string topic][u32 partition][u64 offset][u32 maxRecords][u32 maxBytes]
Response: [u64 highWatermark][u64 startOffset][u32 count]  then count × [bytes record]
```

- `highWatermark`: the offset the next produce will receive, at the time of the response.
- `startOffset`: the offset of the first returned record (always the offset you requested). Record *i* has offset `startOffset + i`; the next fetch should use `startOffset + count`.
- **Limits.** `maxRecords` is clamped to `[1, 4096]`. `maxBytes` of `0` or greater than 32 MiB is treated as 32 MiB. `maxBytes` counts **on-disk size** (payload plus an 8-byte prefix per record), not wire size. At least one record is always returned if any exists at `offset`, even if it is larger than `maxBytes`. You may receive fewer records than requested even when more data exists.
- Fetch never creates topics: a missing topic or partition returns `TopicNotFound`. The topic name is not validated, because it is only used as a lookup key.
- A fetch response must fit in the client's frame limit. The CLI client uses `DefaultMaxFrameBytes` (16 MiB) on responses, so keep `maxBytes` below that for such clients.
- Errors: `MalformedRequest`, `TopicNotFound`, `OffsetOutOfRange` (`[u64 low]`), `OffsetPastEnd` (`[u64 high]`), `Unavailable`, `Internal`.

#### `0x03` CommitOffset

Stores the consumer-group position for a partition.

```
Request : [string group][string topic][u32 partition][u64 offset]
Response: (empty)
```

- By convention `offset` is the **next offset to consume** (`last processed + 1`). The broker stores the value verbatim: it does not check that the topic exists or that the offset is within the log's range.
- Committing overwrites any previous value atomically.
- Errors: `MalformedRequest`, `InvalidTopic` (group *or* topic name invalid), `Internal`.

#### `0x04` FetchOffset

Reads back a committed position.

```
Request : [string group][string topic][u32 partition]
Response: [u64 offset]
```

- Errors: `MalformedRequest`, `OffsetNotFound` (nothing committed; empty payload), `InvalidTopic`, `Internal` (for example a corrupt offset file).

#### `0x05` ListTopics

```
Request : (empty)
Response: [u16 count] then count × [string topic]
```

Topics are returned **sorted** and de-duplicated across partitions. This command never fails with an error code once the frame is valid.

### 9.6 Worked byte-level examples

**Produce `"hi"` to `orders`, partition 0**

```
Request frame (23 bytes):
  00 00 00 13                      length = 19 (1 cmd + 18 payload)
  01                               command  = Produce
  00 06  6F 72 64 65 72 73         topic    = "orders"
  00 00 00 00                      partition = 0
  00 00 00 02  68 69               data     = "hi"

Response frame (13 bytes):
  00 00 00 09                      length = 9 (1 code + 8 payload)
  00                               error code = None
  00 00 00 00 00 00 00 00          assigned offset = 0
```

**Fetch up to 10 records from offset 0 of `orders`/0 with a 1 MiB byte limit**

```
Request frame (33 bytes):
  00 00 00 1D                      length = 29
  02                               command = Fetch
  00 06  6F 72 64 65 72 73         topic = "orders"
  00 00 00 00                      partition = 0
  00 00 00 00 00 00 00 00          offset = 0
  00 00 00 0A                      maxRecords = 10
  00 10 00 00                      maxBytes = 1,048,576
```

A response carrying the two records `"hi"` and `"yo"` (high watermark 2):

```
  00 00 00 21                      length = 33
  00                               error code = None
  00 00 00 00 00 00 00 02          highWatermark = 2
  00 00 00 00 00 00 00 00          startOffset  = 0
  00 00 00 02                      count = 2
  00 00 00 02  68 69               record 0 = "hi"
  00 00 00 02  79 6F               record 1 = "yo"
```

**Fetch past the end** (offset 100 when the log has 1 record): code `0x07`, payload `[u64 1]` (the high watermark).

### 9.7 Writing a client in another language

1. Open a TCP connection; set read/write deadlines on your side.
2. To send a request: build the payload with the primitive encodings above; prepend `[u32 (1 + payloadLen)][u8 command]`.
3. Read 4 bytes → length *L*; read *L* bytes; the first is the error code, the rest the payload.
4. Branch on the code (§9.4). For `0x06`/`0x07`, decode the `u64` watermark from the payload; for other non-zero codes, the payload is a text message (except `0x08`, which is empty).
5. Reuse the connection for subsequent requests, one at a time.

---

## 10. Request Workflows

### 10.1 Produce

1. **Client** encodes the payload (`[topic][partition][data]`) and sends a `Produce` frame.
2. **`Handler.Handle`** reads the frame (enforcing `max_frame_bytes`) and dispatches to `handleProduce`.
3. **`handleProduce`** decodes topic, partition, and data; a decode error returns `MalformedRequest`.
4. **`Store.GetOrCreateLog`** validates the topic name, then looks the log up under a read lock; if absent it takes the write lock, re-checks, and calls `commitlog.Open`, which creates the directory and an initial segment.
5. **`Log.Append`** (write lock): rolls the segment if the active one is full, then calls `Segment.Append`.
6. **`Segment.Append`** (write lock): assigns the offset and position, writes `[length][payload]` to the `.log`, writes the index entry, fsyncs both if `file_sync`, increments `nextOffset`.
7. **`Log.Append`** adds to `totalSize` and runs retention.
8. **`handleProduce`** encodes `[u64 offset]` and returns `ErrCodeNone`.
9. **`Handler.Handle`** writes the response frame; the client decodes the offset.

### 10.2 Fetch

1. **Client** sends `Fetch` with `topic`, `partition`, `offset`, `maxRecords`, `maxBytes`.
2. **`handleFetch`** decodes the request, calls `Store.GetLog` (which never creates); `nil` → `TopicNotFound`.
3. It clamps `maxRecords` to `[1, 4096]` and `maxBytes` to at most 32 MiB.
4. **`Log.ReadBatch`** (read lock): checks the watermarks (`OutOfRange`/`PastEnd`), finds the starting segment by binary search, and reads from successive segments.
5. **`Segment.readBatch`** (segment read lock): index lookup for the first record's position, then sequential `ReadAt` reads honoring count and byte limits.
6. **`handleFetch`** maps errors: `ErrOffsetOutOfRange` → `0x06` + `[low]`; `ErrReadPastEnd` → `0x07` + `[high]`; `ErrLogClosed` → `0x0A`; others → `0x09`.
7. On success it responds `[high][startOffset][count][records…]`.

### 10.3 CommitOffset

1. Client sends `[group][topic][partition][offset]`.
2. `handleCommitOffset` decodes; `Manager.Commit` validates names, writes the temp file, fsyncs, renames, fsyncs the directory.
3. Response: empty payload with `None`, or `InvalidTopic` / `Internal`.

### 10.4 FetchOffset

1. Client sends `[group][topic][partition]` (typically on startup or restart).
2. `handleFetchOffset` calls `Manager.Fetch`, which reads the 8-byte file.
3. Response: `[u64 offset]`; `OffsetNotFound` with an empty payload if nothing has been committed; `InvalidTopic` / `Internal` otherwise.

### 10.5 ListTopics

1. Client sends an empty `ListTopics` frame.
2. `handleListTopics` calls `Store.Topics()` (sorted, unique) and encodes `[u16 count][string]…`.

---

## 11. Concurrency Model

### 11.1 Goroutines

- **Accept loop:** one goroutine per server.
- **Connection handlers:** one goroutine per accepted connection (`go handler.Handle(conn)`), lightweight enough to serve many concurrent clients without OS-thread-per-connection overhead.
- Signal handling is via `signal.NotifyContext` in `main`.

### 11.2 Locks

| Lock | Type | Protects | Held for write by | Held for read by |
| --- | --- | --- | --- | --- |
| `Server.mu` | `Mutex` | listener, active-connection set | accept loop, handler exit, `Stop`, `Addr` | n/a |
| `Store.mu` | `RWMutex` | `logs` map, `closed` flag | creating a log (including its `Open` and recovery), `Close` | `GetLog`, `GetOrCreateLog` fast path, `Topics`, `Partitions` |
| `Log.mu` | `RWMutex` | segment slice, `activeSegment`, `totalSize`, `closed` | `Append` (incl. rollover and retention), `Close` | `ReadBatch`, accessors (`HighestOffset`, `LowestOffset`, `Size`, `SegmentCount`) |
| `Segment.mu` | `RWMutex` | file handles, `nextOffset`, `storeSize` | `Append`, `Close` | `readBatch`, `IsFull`, `NextOffset`, `Size` |
| `index.mu` | `RWMutex` | index file handle and size | `WriteEntry`, `rewrite`, `truncate…`, `Sync`, `Close` | `ReadPositionForOffset`, `entries`, `Name` |
| `offset.Manager.mu` | `RWMutex` | offset-file operations | `Commit` | `Fetch` |

### 11.3 Lock ordering and re-entrancy

- Acquisition order inside the storage engine is always **`Log.mu` → `Segment.mu` → `index.mu`**, never the reverse.
- `Store.mu` is released before a returned `*Log` is used, with one deliberate exception: creating a new log holds the store's write lock through `commitlog.Open`, which serializes concurrent first-time creation (and briefly blocks other store lookups while a log is opened).
- Go's `sync.RWMutex` read locks are **not re-entrant**: acquiring `RLock` twice in one goroutine can deadlock if a writer queues between the two calls. For this reason `Log.ReadBatch` reads the watermarks directly from the segments while it holds the lock instead of calling the locking accessors `LowestOffset()`/`HighestOffset()`. (V1 had exactly this deadlock risk.)

### 11.4 Resulting parallelism

- Different partitions are independent `Log` objects: appends and reads on different partitions run fully in parallel.
- Within one partition, appends are **serialized**; reads run concurrently with each other but wait while an append holds the write lock.
- All consumer-offset commits are serialized with each other (single lock), while fetches run concurrently.

### 11.5 Graceful shutdown mechanics

A `quit` channel (closed once via `sync.Once`) signals shutdown; closing the listener unblocks `Accept`; closing connections unblocks handlers' blocking reads/writes; the `WaitGroup` makes `Stop` wait for every goroutine; per-request deadlines bound any single blocked operation.

---

## 12. Configuration Reference

Configuration is read from a YAML file (default `configs/server.yaml`). The file must exist; any key it omits keeps its built-in default. Byte sizes are plain integers (bytes); durations use Go syntax (`60s`, `2m`).

```yaml
server:
  listen_address: ":9898"
  read_timeout: 60s
  write_timeout: 60s
  max_frame_bytes: 16777216

log:
  data_dir: ./fluxgo-data
  max_segment_bytes: 33554432
  max_log_bytes: 2147483648
  file_sync: true
```

| Key | Type | Built-in default | Description |
| --- | --- | --- | --- |
| `server.listen_address` | string | `127.0.0.1:9898` | Bind address. The shipped `server.yaml` uses `":9898"` (all interfaces). |
| `server.read_timeout` | duration | `60s` | Deadline for reading a complete request frame, reset per request; doubles as idle timeout. `0` disables. |
| `server.write_timeout` | duration | `60s` | Deadline for writing a response frame. `0` disables. |
| `server.max_frame_bytes` | int64 | `16777216` | Upper bound for a request frame, hence for a produced message. |
| `log.data_dir` | string | `./fluxgo-data` | Root directory for all data; resolved to an absolute path at load time. |
| `log.max_segment_bytes` | int64 | `33554432` | Segment rollover threshold. |
| `log.max_log_bytes` | int64 | `2147483648` | Per-partition retention limit (`.log` bytes). `0` disables retention. |
| `log.file_sync` | bool | `true` | fsync log and index after every append. |

### Validation rules

The broker refuses to start if any of these fail:

- `server.listen_address` must not be empty.
- `server.read_timeout` and `server.write_timeout` must not be negative.
- `server.max_frame_bytes` must be between `1024` and `1073741824` (1 GiB).
- `log.data_dir` must not be empty.
- `log.max_segment_bytes` must be positive.
- `log.max_log_bytes` must not be negative.
- If `log.max_log_bytes > 0`, it must be `>= log.max_segment_bytes`.

### Tuning notes

- **Throughput vs. durability:** `file_sync: false` removes two fsyncs per append and is much faster, at the cost of the guarantees in §7.2.
- **Retention granularity:** smaller `max_segment_bytes` means finer-grained deletion but more files and more rollovers.
- **Large messages:** raise `server.max_frame_bytes` (and make sure clients' frame limits accommodate fetch responses).
- **Exposure:** there is no authentication or TLS; use `127.0.0.1:…` or a trusted network.

---

## 13. Error Handling

### 13.1 Conventions

- Errors are propagated up the call stack and wrapped with `fmt.Errorf("…: %w", err)` to preserve context.
- Logical conditions have sentinel errors (`commitlog.ErrReadPastEnd`, `offset.ErrOffsetNotFound`, `validate.ErrInvalidName`, `store.ErrStoreClosed`, …) checked with `errors.Is`.
- Multiple cleanup failures are combined with `errors.Join`.
- The handler translates internal errors into protocol error codes; the broker never exposes Go error strings for internal faults (clients get short generic messages while details go to the log).

### 13.2 Mapping: internal condition → response

| Command | Condition | Response code |
| --- | --- | --- |
| any | unknown command byte | `0x01` UnknownCommand |
| any | payload fails to decode | `0x02` MalformedRequest |
| Produce | invalid topic name | `0x04` InvalidTopic |
| Produce | store closed, log open failure, append failure | `0x09` Internal |
| Fetch | log not found | `0x05` TopicNotFound |
| Fetch | `ErrOffsetOutOfRange` | `0x06` + `[low]` |
| Fetch | `ErrReadPastEnd` | `0x07` + `[high]` |
| Fetch | `ErrLogClosed` | `0x0A` Unavailable |
| Fetch | any other read error (e.g. `ErrCorruptSegment`) | `0x09` Internal |
| CommitOffset | invalid group or topic | `0x04` InvalidTopic |
| CommitOffset | I/O failure | `0x09` Internal |
| FetchOffset | `ErrOffsetNotFound` | `0x08` OffsetNotFound |
| FetchOffset | invalid group or topic | `0x04` InvalidTopic |
| FetchOffset | I/O failure or corrupt file | `0x09` Internal |
| ListTopics | n/a | `0x00` |

### 13.3 Connection-level conditions (no response frame)

| Condition | Behavior |
| --- | --- |
| Frame length 0, or larger than `max_frame_bytes`, or truncated body | Warning logged; connection closed. |
| Peer disconnects, or read/write deadline exceeded | Connection closed (not logged as a warning). |
| Panic while handling a request | Error logged with the panic value; that connection is closed; the broker keeps running. |

---

## 14. Client Guide

### 14.1 Producing

Pick the partition on the client (FluxGo has no key-based partitioner; to preserve per-key ordering, hash the key to a stable partition ID). Send `Produce` and use the returned offset as the record's identity. If a produce fails ambiguously (timeout, disconnect), a retry can create a duplicate (§17).

### 14.2 Consuming: a robust loop

```
next := FetchOffset(group, topic, partition)        // 0x08 → no prior commit; choose a start
loop:
    resp := Fetch(topic, partition, next, maxRecords, maxBytes)
    switch resp.code:
    case None:
        for each record i:  process(record i)       // offset = resp.startOffset + i
        next = resp.startOffset + resp.count
        CommitOffset(group, topic, partition, next) // after processing → at-least-once
    case OffsetPastEnd:                             // caught up: payload = high watermark
        sleep(poll interval); continue
    case OffsetOutOfRange:                          // retention deleted it: payload = low watermark
        next = low watermark; continue              // or alert, if gaps matter to you
    case TopicNotFound:
        sleep or fail                               // nothing produced yet
    default:
        handle / retry
```

Notes:

- **Starting position.** With no committed offset, start at `0`. If retention has already removed offset `0`, you will get `OffsetOutOfRange` and can jump to the low watermark from the response.
- **Commit semantics.** Commit the offset of the **next** record to read. Committing *after* processing yields at-least-once; committing *before* yields at-most-once.
- **Batching the commit.** Committing once per batch (not per record) reduces fsync cost on the broker.
- **Polling.** Fetch is non-blocking, so use a short sleep when you receive `OffsetPastEnd`. (The CLI's `-follow` uses 500 ms.)
- **Same-group consumers.** The broker does not assign partitions within a group; run at most one consumer per `(group, topic, partition)`.
- **The high watermark** in a successful response lets you compute lag: `highWatermark − (startOffset + count)`.

### 14.3 The CLI as a reference client

`cmd/fluxgo-client/main.go` is a compact reference implementation of all five commands, including the `OffsetPastEnd` / `OffsetOutOfRange` handling in `fetch -follow`.

---

## 15. Logging and Observability

FluxGo logs with `log/slog` (text format, stdout). The default level is `Info`; `-verbose` enables `Debug`. Logs carry structured key/value pairs (`topic`, `partition`, `offset`, `remote`, …).

| Level | Representative events |
| --- | --- |
| `INFO` | `configuration loaded`; `broker listening`; `store: loaded log` (with low/high watermark); `store: finished loading`; `store: created log`; `commitlog: applying retention` (segments removed, bytes freed); `broker stopped`; `shutdown complete`. |
| `WARN` | `commitlog: truncating partial record tail`; `commitlog: index out of sync with log, rebuilding`; `commitlog: rebuilt index from log`; `commitlog: ignoring file with invalid name`; `store: skipping directory with invalid name`; `failed to read request`; `failed to write response`; `accept failed`; failure to sync before rollover. |
| `ERROR` | `panic while serving connection, dropping it`; `produce: append failed`; `fetch: read failed`; `offset commit failed` / `offset fetch failed`; `commitlog: retention failed`; failure to close a log or the store. |
| `DEBUG` | `connection opened`, `connection closed`. |

The recovery `WARN` messages are the main signal that a restart followed an unclean shutdown. There is currently no metrics endpoint.

---

## 16. Testing

```sh
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

The suite has **35 test functions** across five packages. Integration tests run a real broker on `127.0.0.1:0` with a temporary data directory (`t.TempDir()`), and storage tests use tiny segment sizes to force rollovers.

### `internal/commitlog` (12)

| Test | Verifies |
| --- | --- |
| `TestAppendReadRoundTrip` | Basic append and read. |
| `TestReadPastEndAndEmptyMessage` | `ErrReadPastEnd` semantics; empty records are valid. |
| `TestSegmentRollover` | Rollover behavior and reads across segments. |
| `TestReopenContinuesAppending` | **Regression** for the V1 bug where post-restart appends overwrote the index. |
| `TestRecoveryAdoptsUnindexedTail` | Crash after log write but before index write → record adopted. |
| `TestRecoveryRebuildsIndex` | Truncated index is rebuilt from the log. |
| `TestRecoveryTruncatesPartialRecord` | Torn record tail is truncated. |
| `TestRetentionDeletesOldSegments` | Oldest segments are removed and the low watermark advances. |
| `TestReadBatch` | Cross-segment batches, count limit, byte limit (16 bytes/record accounting), tiny `maxBytes` still returns one record. |
| `TestClosedLog` | `ErrLogClosed` on use after close; double close is safe. |
| `TestConcurrentAppendRead` | 8 writers × 50 appends: correct final watermark and every record readable. |
| `TestOrphanIndexFileIsIgnored` | An `.index` with no `.log` creates no phantom segment. |

### `internal/broker` (6, end-to-end over TCP)

| Test | Verifies |
| --- | --- |
| `TestProduceFetchEndToEnd` | 60 produces across segment rollovers; full and mid-log fetch with correct watermark and start offset. |
| `TestFetchPastEndAndUnknownTopic` | `OffsetPastEnd` (carrying the high watermark) and `TopicNotFound`. |
| `TestOffsetCommitFetchOverWire` | `OffsetNotFound` before commit; commit then fetch returns the value. |
| `TestListTopics` | Topics are returned sorted and de-duplicated across partitions. |
| `TestMalformedRequestsDoNotCrashServer` | Truncated payloads for every command and an unknown command produce error responses, and the server stays healthy. |
| `TestInvalidTopicRejected` | A `../escape` topic returns `InvalidTopic`. |

### `internal/protocol` (7)

`TestFrameRoundTrip`, `TestFrameEmptyPayload`, `TestReadFrameRejectsOversized`, `TestReadFrameRejectsTruncated`, `TestEncoderDecoderRoundTrip`, `TestDecoderShortReads`, `TestErrorCodeToStringCoversAll`.

### `internal/store` (7)

`TestFormatParseLogNameRoundTrip`, `TestParseLogNameRejectsGarbage`, `TestGetOrCreateAndReload`, `TestGetLogDoesNotCreate`, `TestInvalidTopicNamesRejected`, `TestTopicsAndPartitions`, `TestClosedStore`.

### `internal/offset` (3)

`TestCommitFetchRoundTrip` (including overwrite and group/topic/partition independence), `TestPersistenceAcrossManagers`, `TestInvalidNamesRejected`.

> `internal/config` and `internal/validate` do not have dedicated test files; their behavior is exercised indirectly (for example by the invalid-name tests in `store`, `offset`, and the broker integration tests).

---

## 17. Limitations and Non-Goals

FluxGo V2 is intentionally small. These are scope cuts, not oversights.

- **Single node.** No replication, clustering, leader election, or fault tolerance. The broker is a single point of failure.
- **No consumer-group coordination.** The broker stores offsets per group but does not manage group membership, assign partitions to consumers, or rebalance on join/leave/failure. Multiple consumers of the same group reading the same partition will interfere with each other's commits, causing duplicate or skipped processing.
- **Basic retention only.** Size-based, whole-segment retention. No time-based retention, no log compaction.
- **No security.** No authentication, authorization, or TLS.
- **No transactions or exactly-once.** Delivery is effectively at-least-once (a producer retry after an ambiguous failure can duplicate a record, including one that crash recovery adopted) or at-most-once (no retry). There is no idempotent producer or deduplication.
- **No integrity checksums.** Torn tails are detected by length only (§7.5).
- **Pull model, no long polling.** Fetch returns immediately; consumers poll.
- **Produce is one record per request.** Fetch is batched; produce is not.
- **Opaque records.** No keys, headers, timestamps, or compression. The client chooses the partition.
- **Minimal metadata.** `ListTopics` returns names only; partition lists, watermarks, and broker info are not available over the wire (the store can enumerate partitions internally).
- **Serialized appends per partition**, and a single lock for consumer-offset commits.
- **Reserved error code.** `MessageTooLarge (0x03)` is defined but unused; oversized frames close the connection.
- **Not wire-compatible with V1.**

If you need these features, you need Kafka.

---

## 18. Future Directions

Ideas, not commitments:

- **Consumer-group coordination and rebalancing:** membership tracking, exclusive partition assignment within a group, rebalance on membership change.
- **Richer metadata API:** partitions per topic, per-partition watermarks, broker information.
- **Batched produce** to complement batched fetch.
- **Time-based retention** (and possibly compaction).
- **Performance work:** buffer pooling with `sync.Pool`, fewer allocations on the read path.
- **Observability:** metrics endpoint.
- *(Long term)* **Replication** (primary/backup or Raft-based) and cluster coordination.

---

## 19. V1 → V2 Changelog

V2 keeps V1's scope and model and fixes its problems.

| Area | V1 | V2 |
| --- | --- | --- |
| Index file on reopen | Opened without `O_APPEND` and written with plain `Write`, so **post-restart appends overwrote the index from byte 0**. | Positional `WriteAt` at the tracked size; entry order validated. |
| Crash recovery | `nextOffset` inferred from index size; torn log data never cleaned up. | Log scan reconciles both files; torn tails truncated; stale or truncated indexes rebuilt. |
| Retention | `applyRetention` was defined but never called. | Enforced after every append. |
| Deadlock risk | `Log.Read` took `RLock` and then called `HighestOffset()` (a second `RLock`); `sync.RWMutex` read locks are not re-entrant once a writer queues. | Watermarks are read directly while the lock is held. |
| Topic names | Split at the first `_` (broke topics containing `_`); no validation, so `../` could escape the data directory. | Split at the last `_`; strict `[a-zA-Z0-9._-]` validation shared by the store and offset manager. |
| Consume | One record per RPC. | Batched `Fetch` with count and byte limits and watermarks. |
| Errors | One generic "offset invalid" code. | Distinct past-end vs out-of-range codes carrying resync watermarks. |
| Payload parsing | Manual cursor arithmetic. | `Encoder`/`Decoder` helpers with sticky errors. |
| Handler crash | A panic killed the process. | Per-connection `recover`. |
| Metadata | None. | `ListTopics`. |
| Partition IDs | 64-bit on the wire. | 32-bit. |
| Misc | Unused `Config.Path`, empty `partition.go`, empty `fluxgo.proto`, empty `SanityCheck`, `fmt.Printf` logging, hard-coded 10 MB frame cap, data dir committed to the repo. | Removed; `log/slog`; configurable `server.max_frame_bytes`; data dir git-ignored. |
| Interfaces | `Log` interface plus `commitLog` struct. | Concrete `commitlog.Log` type. |
| CLI | `consume` action (single record). | `fetch` (batched, `-follow`), `fetch-offset`, `topics`. |
| Tests | None. | Unit, recovery, retention, concurrency, and TCP integration tests. |

### Compatibility notes

- **Wire protocol:** not compatible. Batch fetch replaced single-message consume, partition IDs are 32-bit, and error codes were reworked. V1 clients cannot talk to a V2 broker, or vice versa.
- **On-disk layout:** the structure is unchanged (`<topic>_<partition>/` directories, `<base>.log` / `<base>.index` segment pairs with the same record and entry formats, `__consumer_offsets/<group>/<topic>_<partition>.offset` files). Because V2 recovery rebuilds any index that disagrees with its log, an existing V1 data directory is expected to open under V2; this migration path is not covered by the test suite, so back up a V1 data directory before pointing V2 at it.

---

## 20. Glossary

| Term | Definition |
| --- | --- |
| **Active segment** | The newest segment of a partition; the only one that receives appends and the one retention never deletes. |
| **Base offset** | The absolute offset of the first record in a segment; used as its file name. |
| **Broker** | The FluxGo server process. |
| **Commit log** | The append-only, segmented storage structure for one partition. |
| **Consumer group** | A named identity under which a consumer stores its offsets on the broker. FluxGo does not coordinate group members. |
| **Frame** | One length-prefixed protocol message (request or response). |
| **High watermark** | The offset the next appended record will receive (one past the last record). |
| **Low watermark** | The oldest offset still retained. |
| **Offset** | The sequential 64-bit position of a record within a partition. |
| **Partition** | An independent, ordered log within a topic. |
| **Reconcile** | Crash-recovery step in which a segment's `.log` is scanned and the `.index` is brought into agreement with it. |
| **Record** | A single opaque message payload stored in a partition. |
| **Relative offset** | A record's offset minus its segment's base offset; the index key. |
| **Retention** | Deleting the oldest whole segments when a partition exceeds `max_log_bytes`. |
| **Sealed segment** | A non-active segment that was closed by rollover and will no longer be written to. |
| **Segment** | One `.log` + `.index` file pair covering a contiguous offset range. |
| **Topic** | A named feed of records, made of one or more partitions. |
| **Torn tail** | A partially written record at the end of a `.log` file after a crash. |
