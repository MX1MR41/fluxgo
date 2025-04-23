# FluxGo - Technical Documentation

## 1. Introduction

FluxGo is a message broker and distributed log system designed and built in Go, drawing inspiration from Apache Kafka. Its primary objective is to provide a subset of Kafka's core functionalities – specifically persistent topic-partition logs with produce/consume capabilities – while prioritizing performance, simplicity, and leveraging the idiomatic strengths of the Go language.

This document provides an in-depth technical explanation of FluxGo's V1 architecture, components, data flow, protocol, and design rationale. It is intended for developers seeking to understand the internal workings of FluxGo, potentially for extension, learning, or analysis.

## 2. Core Philosophy & Model: The Distributed Log

FluxGo adopts the **distributed log** abstraction as its core model, similar to Kafka. This differs significantly from traditional point-to-point message queues:

*   **Append-Only Log:** Data for each topic partition is stored as an ordered, immutable sequence of records appended to one or more segment files on disk.
*   **Offsets:** Each record within a partition log is assigned a unique, monotonically increasing 64-bit integer offset. This offset serves as the primary coordinate for consumers.
*   **Consumer Responsibility:** Consumers are responsible for tracking the offset up to which they have processed data for each partition they subscribe to. They request data *from* a specific offset.
*   **Data Retention, Not Deletion on Read:** Messages are *not* deleted when read by a consumer. Instead, they are removed from the log based on configured retention policies (currently, total log size). This allows multiple consumers or consumer groups to read the same data independently and enables message replayability.
*   **Publisher/Subscriber:** This log model naturally facilitates a publish/subscribe pattern. Producers publish to topics, and multiple independent consumers can subscribe and read from those topics at their own pace.

**Why this model?**

*   **High Throughput:** Sequential disk writes (appending to logs) are generally much faster than random writes required by complex indexing or message state management in some P2P queues. Sequential reads are also highly efficient and benefit from OS page caching.
*   **Decoupling & Replayability:** Consumers are fully decoupled. The broker doesn't track complex per-message delivery states. New consumers can join and read history (within retention limits). Data can be reprocessed by resetting offsets.
*   **Scalability:** Partitioning allows distributing a topic's load across multiple logs (and potentially multiple brokers in future versions).

**Go Buff:** Go's efficiency with I/O and its simple, explicit error handling make implementing the low-level log storage manageable and potentially very performant.

## 3. System Architecture (V1)

FluxGo V1 operates as a single‑node broker. The high‑level architecture consists of the following layers and components:


```
                       +-----------------------+
Producer Client  --->  |   FluxGo Broker       |  <---  Consumer Client
                       +-----------------------+
                              |       ^
                              v       |
                       +-----------------------+
                       | Network Listener      |
                       | (internal/broker)     |
                       +-----------------------+
                              |       ^
                              v       |
                       +-----------------------+
                       | Connection Handler    |
                       | (internal/broker)     |
                       +-----------------------+
                              |       ^
                              v       |
                       +-----------------------+
                       | Store Manager         |
                       | (internal/store)      |
                       +-----------------------+
                              |       ^
                              v       |
                       +-----------------------+
                       | Commit Log            |
                       | (internal/commitlog)  |
                       +-----------------------+
                              |       
                              v       
                       +-----------------------+
                       | Disk Storage          |
                       +-----------------------+
```

- **Networking** (`internal/broker`): Handles TCP connections, reads requests, writes responses.  
- **Protocol** (`internal/protocol`): Defines the binary wire format for communication.  
- **Request Handling** (`internal/broker/handler.go`): Parses requests, interacts with the storage layer, formats responses.  
- **Log Management** (`internal/store`): Manages the lifecycle and access to different topic‑partition logs.  
- **Persistence** (`internal/commitlog`): The core log storage engine, dealing with file segments, indexing, appends, and reads.  
- **Configuration** (`internal/config`): Loads server settings.  

## 4. Component Deep Dive

### 4.1. `internal/commitlog` - The Persistence Engine

This package is the heart of FluxGo, responsible for durable, ordered storage of messages for a *single partition*.

*   **`Log` Interface:** Defines the contract for a commit log (`Append`, `Read`, `Close`, `HighestOffset`, `LowestOffset`, etc.).
*   **`commitLog` Struct:** The primary implementation of the `Log` interface.
    *   **Responsibility:** Manages a collection of `Segment` instances for a single partition, handles segment rollovers, loads segments from disk, orchestrates reads across segments, and applies retention policies.
    *   **State:** Holds the directory path, configuration, a sorted slice of active `*Segment`s, and a pointer to the current `activeSegment` used for writes.
    *   **Loading (`loadSegments`):** Scans the log directory for `.log`/`.index` files, parses base offsets from filenames, sorts them, and initializes `Segment` objects for each pair found. Calculates the `nextOffset` for the active segment based on the index size, providing basic crash recovery.
    *   **Appending (`Append`):** Acquires a write lock. Checks if the `activeSegment` is full (`IsFull`). If so, calls `rollSegment`. Delegates the actual append to the `activeSegment.Append` method. Updates internal `totalSize`.
    *   **Rolling (`rollSegment`):** Creates a new `Segment` instance with the next available base offset (`activeSegment.NextOffset()`). Appends this new segment to the `segments` slice and updates the `activeSegment` pointer.
    *   **Reading (`Read`):** Acquires a read lock. Uses `findSegment` (binary search) to efficiently locate the `Segment` responsible for the requested offset. Delegates the read to `segment.Read`.
    *   **Segment Lookup (`findSegment`):** Performs a binary search (`sort.Search`) on the sorted `segments` slice to quickly find the potential segment containing the target offset. This is much faster than linear scanning, especially with many segments.
    *   **Retention (`applyRetention`):** When triggered (currently manual, based on `MaxLogBytes`), identifies the oldest segments (excluding the active one) needed to bring the total size below the limit. Closes and removes the files for these segments. Requires external write lock.
    *   **Concurrency:** Uses `sync.RWMutex` to allow concurrent reads while ensuring exclusive access for appends, rollovers, and closing.
*   **`Segment` Struct:** Represents a single pair of `.log` and `.index` files on disk.
    *   **Responsibility:** Manages I/O for a specific range of offsets (from `baseOffset` up to `nextOffset-1`). Handles writing records and index entries, reading records by offset, and checking if the segment is full.
    *   **Files:** Manages `*os.File` handles for the `.log` (data) and `.index` files. Filenames are zero-padded base offsets (e.g., `00000000000000000123.log`).
    *   **`.log` Format:** `[8-byte BigEndian Length N][N bytes of Record Data][...]`
    *   **`.index` Format:** `[8-byte BigEndian Relative Offset][8-byte BigEndian Position in .log file][...]`. Index entries map the message's offset *relative* to the segment's `baseOffset` to the starting byte position of the message's *length prefix* in the corresponding `.log` file. An entry exists for *every* message.
    *   **Appending (`Append`):** Calculates relative offset and current file position. Writes the length prefix, then the record data to the `.log` file. Writes the corresponding (relative offset, position) entry to the `.index` file. If `FileSync` config is true, calls `Sync()` on both file handles to flush OS buffers to disk (ensuring durability at the cost of performance). Updates internal file size and `nextOffset`. **Critical Section:** A crash between the `.log` write and the `.index` write can lead to unindexed data (which might be overwritten on restart). V1 accepts this risk; more robust recovery could scan the log tail.
    *   **Reading (`Read`):** Calculates relative offset. Reads the index entry using `index.ReadPositionForOffset`. Reads the 8-byte length prefix from the `.log` file at the retrieved position using `ReadAt`. Reads the record data itself from the position immediately following the length prefix using `ReadAt`. `ReadAt` avoids seeking.
    *   **Concurrency:** Uses `sync.RWMutex` to protect file handles and internal state (`nextOffset`, `storeSize`).
*   **`index` Struct:** Helper to manage the `.index` file. Primarily provides `ReadPositionForOffset` and `WriteEntry`. Uses `ReadAt` for reads and `Write` (append) for writes.
*   **Go Buffs:**
    *   `os` package: Direct and efficient file system operations are crucial. `Sync()` provides durability control. `ReadAt`/`WriteAt` allow positional I/O without seeking.
    *   `encoding/binary`: Compact and fast serialization for fixed-size numeric data in files and network protocol.
    *   `sync.RWMutex`: Enables high read concurrency while maintaining safety for writes in both `commitLog` and `Segment`. Essential for performance.
    *   `sort.Search`: Provides efficient O(log N) segment lookup in `commitLog.findSegment`.
    *   Explicit Error Handling: Go's error handling makes dealing with I/O failures explicit and manageable.

### 4.2. `internal/store` - Log Aggregation & Management

*   **Responsibility:** Manages the collection of `commitlog.Log` instances for *all* topic-partitions served by the broker. Acts as the entry point for the `Handler` to access the correct log.
*   **`Store` Struct:** Holds the base data directory, server configuration, and a `map[string]commitlog.Log` where the key is the log name (e.g., "orders_0").
*   **Log Naming Convention:** Assumes subdirectories within the `baseDir` named `topic_partition` correspond to individual commit logs.
*   **Loading (`loadExistingLogs`):** Scans the `baseDir` on startup, identifies directories matching the naming convention, and calls `commitlog.NewLog` for each one to load existing data. Populates the internal `logs` map.
*   **Access (`GetLog`, `GetOrCreateLog`):** Provides methods to retrieve log instances. `GetOrCreateLog` is the primary access method, using a double-checked locking pattern with the RWMutex to efficiently handle concurrent requests: check with read lock, if not found, acquire write lock, check again (in case another goroutine created it), and create if still necessary using `commitlog.NewLog`.
*   **Configuration:** Uses `config.ServerConfig` to pass the appropriate `commitlog.Config` (derived via `GetCommitLogConfig`) when creating new `commitlog.Log` instances.
*   **Concurrency:** Uses `sync.RWMutex` to protect the `logs` map.

### 4.3. `internal/protocol` - Wire Format Contract

*   **Responsibility:** Defines the binary format for data exchanged over the network between clients and the broker. Ensures both sides understand how to structure and parse messages.
*   **Key Elements:**
    *   Constants for frame structure (`LenPrefixSize`, `CmdCodeSize`, `ErrCodeSize`).
    *   Constants for command codes (`CmdProduce`, `CmdConsume`).
    *   Constants for error codes (`ErrCodeNone`, `ErrCodeUnknownCommand`, etc.) providing granular error information.
    *   `ErrorCodeToString` helper for human-readable errors.
    *   Helper functions (`WriteFrame`, `ReadFrame`) encapsulate the logic of writing/reading length-prefixed messages, handling command/error codes, and basic validation.
*   **V1 Format:** Simple length-prefixing. All multi-byte numbers use Big Endian. Specific payload formats are defined conceptually within the handler/client code for Produce/Consume.
*   **Go Buffs:** `encoding/binary` for efficient serialization. Go's strong typing helps ensure constants are used correctly.

### 4.4. `internal/broker` - Network Interaction & Request Orchestration

*   **Responsibility:** Handles the network-facing aspects of the broker: listening for connections, managing connection lifecycles, and orchestrating the request/response flow.
*   **`Server` Struct:** The main server object.
    *   **State:** Holds configuration, the `net.Listener`, the `store.Store`, a `sync.WaitGroup` for graceful shutdown, a `quit` channel for signaling, and a map of active connections (`activeConns`).
    *   **Listening (`Start`):** Creates the TCP listener via `net.Listen`. Launches the `acceptLoop` goroutine. Waits for shutdown signals (context cancellation or `quit` channel).
    *   **Accepting Connections (`acceptLoop`):** Runs in a loop, calling `listener.Accept()`. For each successful connection, it adds the connection to `activeConns` (under mutex protection), increments the `WaitGroup`, and launches a new goroutine executing `handler.Handle(conn, ...)` .
    *   **Shutdown (`Stop`):** Closes the `quit` channel to signal loops to stop. Closes the `net.Listener` to stop accepting new connections. Iterates through `activeConns` and closes each connection (interrupting blocking reads/writes in handlers). Waits on the `sync.WaitGroup` for all handler goroutines and the `acceptLoop` to finish.
*   **`Handler` Struct:** Handles the logic for a *single* client connection.
    *   **State:** Holds a reference to the `store.Store`.
    *   **Processing Loop (`Handle`):** Runs in its own goroutine. Continuously:
        1.  Sets read deadline.
        2.  Reads the request frame (length + cmd + payload) using `bufio.Reader` and `io.ReadFull`.
        3.  Parses command code.
        4.  Uses a `switch` statement to dispatch to specific command handlers (`handleProduce`, `handleConsume`).
        5.  Calls the appropriate handler function.
        6.  Constructs the response frame (length + error code + payload).
        7.  Sets write deadline.
        8.  Writes the response frame.
    *   **Command Handlers (`handleProduce`, `handleConsume`):** Implement the logic for specific commands: parse the request payload, interact with the `store` to get the correct `commitlog.Log`, perform the log operation (`Append` or `Read`), construct the response payload (e.g., offset for produce, record data for consume), and return the payload and an appropriate error code.
*   **Go Buffs:**
    *   **Goroutines:** The `goroutine-per-connection` model in `acceptLoop` is a cornerstone of Go's concurrency approach, allowing efficient handling of numerous concurrent clients.
    *   `net` Package: Provides robust and performant TCP networking primitives.
    *   `sync.WaitGroup`: Essential for reliable graceful shutdown.
    *   Channels (`quit`): Idiomatic way to signal shutdown across goroutines.
    *   `bufio`: Used for buffered I/O, potentially improving performance for network reads.
    *   Timeouts (`SetReadDeadline`, `SetWriteDeadline`): Prevent connections from hanging indefinitely.

### 4.5. `internal/config` - Server Configuration

*   **Responsibility:** Loads and provides access to server configuration settings defined in a YAML file.
*   **`ServerConfig` Struct:** Holds nested structs (`ServerSettings`, `LogSettings`) mapping to the YAML structure using `yaml` tags.
*   **Loading (`LoadConfig`):** Sets default values first. Reads the specified YAML file. Uses `gopkg.in/yaml.v3` to unmarshal the file content into the `ServerConfig` struct, overwriting defaults. Performs basic validation/adjustments (e.g., making `DataDir` absolute).
*   **Commit Log Config (`GetCommitLogConfig`):** Provides a bridge between the global `LogSettings` and the specific `commitlog.Config` needed by the `commitlog` package, injecting the correct path for the specific log instance.

### 4.6. `internal/offset` - Consumer Offset Persistence

*   **Responsibility:** Manages the persistent storage of the last processed offset for consumer groups on a per-topic-partition basis. Allows consumers to resume processing from where they left off.
*   **`Manager` Struct:** Orchestrates offset storage. Holds the base directory for offset files.
*   **Storage Mechanism (V1):** File-based. Each committed offset is stored in a separate file.
    *   **Directory Structure:** `<data_dir>/__consumer_offsets/<groupID>/<topic>_<partition>.offset`
    *   **File Content:** The 8-byte Big Endian binary representation of the committed offset value.
*   **Committing (`Commit`):** Writes the offset to a temporary file, syncs it to disk, and then atomically renames it to the final target file path. This provides crash consistency for single offset commits.
*   **Fetching (`Fetch`):** Reads the 8-byte offset value from the corresponding file. Returns a specific `ErrOffsetNotFound` if the file does not exist for the given group/topic/partition.
*   **Concurrency:** Uses a mutex (primarily for potential future enhancements), but atomicity currently relies on `os.Rename`.

## 5. Data Flow / Workflows (V1)

### 5.1. Produce Workflow

1.  **Client:** Constructs the Produce request payload (`[TopicLen][Topic][PartitionID][Data]`).
2.  **Client:** Calls `proto.WriteFrame` to send the length-prefixed request (`CmdProduce`) over the TCP connection.
3.  **Broker (`Handler.Handle`):** Reads the frame using `io.ReadFull`.
4.  **Broker (`Handler.Handle`):** Dispatches to `handleProduce` based on `CmdProduce`.
5.  **Broker (`handleProduce`):** Parses topic, partition, and message data from the payload.
6.  **Broker (`handleProduce`):** Calls `store.GetOrCreateLog(topic, partition)`.
7.  **Broker (`Store`):** Looks up the log. If not found (and under write lock), calls `commitlog.NewLog` to create/load it, passing config from `ServerConfig`. Adds to map. Returns the `commitlog.Log` instance.
8.  **Broker (`handleProduce`):** Calls `log.Append(messageData)`.
9.  **Broker (`CommitLog`):** (Under write lock) Checks if active segment is full. If yes, calls `rollSegment`. Delegates to `activeSegment.Append(messageData)`.
10. **Broker (`Segment`):** (Under write lock) Calculates relative offset & position. Writes length+data to `.log`. Writes offset+position to `.index`. Calls `Sync()` if `FileSync` is true. Increments internal `nextOffset`. Returns absolute offset.
11. **Broker (`handleProduce`):** Receives the assigned absolute offset.
12. **Broker (`handleProduce`):** Constructs the response payload (`[8b Offset]`) and sets error code to `ErrCodeNone`.
13. **Broker (`Handler.Handle`):** Calls `conn.Write` to send the length-prefixed response frame.
14. **Client:** Reads the response frame using `proto.ReadFrame`.
15. **Client:** Checks error code. If `ErrCodeNone`, parses the offset from the response payload.

### 5.2. Consume Workflow

1.  **Client:** Constructs the Consume request payload (`[TopicLen][Topic][PartitionID][Offset]`).
2.  **Client:** Calls `proto.WriteFrame` to send the length-prefixed request (`CmdConsume`).
3.  **Broker (`Handler.Handle`):** Reads the frame.
4.  **Broker (`Handler.Handle`):** Dispatches to `handleConsume` based on `CmdConsume`.
5.  **Broker (`handleConsume`):** Parses topic, partition, and requested offset.
6.  **Broker (`handleConsume`):** Calls `store.GetLog(topic, partition)` (does *not* create).
7.  **Broker (`Store`):** (Under read lock) Looks up the log in its map. Returns the `commitlog.Log` instance or `nil` if not found.
8.  **Broker (`handleConsume`):** If log is `nil`, sets error code to `ErrCodeConsumeTopicNotFound` and prepares error response.
9.  **Broker (`handleConsume`):** If log exists, calls `log.Read(requestedOffset)`.
10. **Broker (`CommitLog`):** (Under read lock) Calls `findSegment(requestedOffset)` to locate the correct segment via binary search.
11. **Broker (`CommitLog`):** If segment found, delegates to `segment.Read(requestedOffset)`.
12. **Broker (`Segment`):** (Under read lock) Calculates relative offset. Reads index entry via `index.ReadPositionForOffset` to get position. Reads length prefix from `.log` at position. Reads record data from `.log` after length prefix. Returns the data (`Record`).
13. **Broker (`handleConsume`):** Receives the record data or an error (e.g., `ErrOffsetNotFound`, `ErrReadPastEnd`).
14. **Broker (`handleConsume`):** If successful, uses the record data as the response payload and sets error code to `ErrCodeNone`. If `ErrOffsetNotFound`/`ErrReadPastEnd`, sets `ErrCodeConsumeOffsetInvalid`. For other errors, sets `ErrCodeConsumeReadFailed`.
15. **Broker (`Handler.Handle`):** Sends the length-prefixed response frame.
16. **Client:** Reads the response frame using `proto.ReadFrame`.
17. **Client:** Checks error code. If `ErrCodeNone`, uses the response payload as the message data. Handles specific error codes like `ErrCodeConsumeOffsetInvalid`.

### 5.3. Commit Offset Workflow

1.  **Client:** Determines the next offset to consume (typically `last_processed_offset + 1`).
2.  **Client:** Constructs the CommitOffset request payload (`[GroupIDLen][GroupID][TopicLen][Topic][PartitionID][OffsetToCommit]`).
3.  **Client:** Calls `proto.WriteFrame` to send the length-prefixed request (`CmdCommitOffset`).
4.  **Broker (`Handler.Handle`):** Reads the frame.
5.  **Broker (`Handler.Handle`):** Dispatches to `handleCommitOffset`.
6.  **Broker (`handleCommitOffset`):** Parses GroupID, Topic, Partition, and Offset from the payload.
7.  **Broker (`handleCommitOffset`):** Calls `offsetManager.Commit(...)`.
8.  **Broker (`OffsetManager`):** Calculates file path (`__consumer_offsets/...`). Writes offset to temp file, syncs, renames to final path. Returns success/error.
9.  **Broker (`handleCommitOffset`):** Constructs response (empty payload on success) with appropriate error code (`ErrCodeNone` or `ErrCodeOffsetCommitFailed`).
10. **Broker (`Handler.Handle`):** Sends the response frame.
11. **Client:** Reads response frame, checks error code.

### 5.4. Fetch Offset Workflow

1.  **Client:** (Typically on startup/restart) Constructs the FetchOffset request payload (`[GroupIDLen][GroupID][TopicLen][Topic][PartitionID]`).
2.  **Client:** Calls `proto.WriteFrame` to send the length-prefixed request (`CmdFetchOffset`).
3.  **Broker (`Handler.Handle`):** Reads the frame.
4.  **Broker (`Handler.Handle`):** Dispatches to `handleFetchOffset`.
5.  **Broker (`handleFetchOffset`):** Parses GroupID, Topic, and Partition.
6.  **Broker (`handleFetchOffset`):** Calls `offsetManager.Fetch(...)`.
7.  **Broker (`OffsetManager`):** Calculates file path. Reads the 8-byte offset from the file. Returns offset or `ErrOffsetNotFound`.
8.  **Broker (`handleFetchOffset`):** Constructs response. If successful, payload is `[8b FetchedOffset]`, error code `ErrCodeNone`. If not found, payload is empty, error code `ErrCodeOffsetNotFound`. For other errors, sets `ErrCodeOffsetFetchFailed`.
9.  **Broker (`Handler.Handle`):** Sends the response frame.
10. **Client:** Reads response frame. Checks error code. If successful, parses the offset from the payload to determine where to start consuming.

## 6. Concurrency Model & Go Buffs

FluxGo heavily relies on Go's concurrency features:

*   **Goroutine-per-Connection:** The broker's `acceptLoop` spawns a new goroutine for each accepted TCP connection (`go handler.Handle(...)`). This is lightweight and allows the server to scale to many concurrent clients efficiently without the heavy overhead of OS threads used in some other languages.
*   **Shared State Protection:** Critical shared resources (the `Store`'s map of logs, the `commitLog`'s slice of segments, the `Segment`'s file handles and state) are protected using `sync.RWMutex`. This allows concurrent reads (e.g., multiple consumers reading from different segments or the store map simultaneously) while ensuring writes (appends, creating logs, segment rollovers) have exclusive access, preventing data corruption.
*   **Graceful Shutdown:** `sync.WaitGroup` ensures all active connection handlers finish processing before the server exits. A `quit` channel is used to signal shutdown intent across goroutines (the accept loop and the main server routine). Closing network listeners and connections helps interrupt blocking operations.
*   **Channels:** Used primarily for shutdown signaling (`quit` channel).

## 7. Persistence Details

*   **Data Directory:** Configured via `log.data_dir`.
*   **Log Directory:** Each topic-partition gets its own subdirectory within `data_dir`, named `topic_partition` (e.g., `fluxgo-data/orders_0`).
*   **Segment Files:** Within each log directory, data is stored in segment files:
    *   `.log` file: Contains the raw message data, prefixed by length. Append-only.
    *   `.index` file: Contains the offset index entries mapping relative offsets to byte positions in the `.log` file. Append-only.
*   **File Naming:** Segment files are named using their base offset, zero-padded to 20 digits (e.g., `00000000000000000000.log`, `00000000000000015000.index`). This ensures lexicographical sorting matches chronological order.
*   **Durability:** Controlled by the `log.file_sync` configuration option. If `true`, `os.File.Sync()` is called after writes to both `.log` and `.index` files within `Segment.Append`, forcing the OS to flush data to disk. This is safer but impacts performance. If `false`, durability relies on the OS's background flushing mechanisms (faster but risks data loss on sudden power loss or OS crash).
*   **Consumer Offsets Directory:** A dedicated subdirectory named `__consumer_offsets` is created within the main `data_dir`. Offsets committed by consumer groups are stored here.
    *   **Path Structure:** `<data_dir>/__consumer_offsets/<groupID>/<topic>_<partition>.offset`
    *   **File Content:** Each `.offset` file contains exactly 8 bytes, representing the `uint64` committed offset in Big Endian format.

## 8. Configuration Details

Configuration is loaded from a YAML file (default: `configs/server.yaml`).

*   `server.listen_address`: TCP address and port for the broker to listen on (e.g., `"127.0.0.1:9898"`, `":9898"`).
*   `server.read_timeout`: Max duration to wait for reading a complete request frame from a client.
*   `server.write_timeout`: Max duration to wait for writing a complete response frame to a client.
*   `log.data_dir`: Path to the base directory where all topic-partition log data will be stored.
*   `log.max_segment_bytes`: Maximum size (in bytes) for a single `.log` file. When reached, the current segment is closed, and a new one is created.
*   `log.max_log_bytes`: Maximum total size (in bytes) for all `.log` files within a single partition's directory. If exceeded after an append, the oldest segments are deleted until the total size is below the limit (basic size-based retention). Set to 0 for no size limit.
*   `log.file_sync`: Boolean (`true`/`false`) controlling whether `fsync` is called after writes for durability.

## 9. Error Handling

*   Errors from file I/O (`os` package) or network operations (`net` package) are generally propagated up the call stack using Go's standard error handling.
*   `fmt.Errorf` with `%w` is used to wrap errors, preserving context.
*   Specific error variables (e.g., `commitlog.ErrOffsetNotFound`) are defined for common logical errors within the commit log. `errors.Is` is used to check for these specific error types.
*   The network protocol defines specific `ErrCode...` constants (`internal/protocol/codes.go`) which are sent back to the client in the response frame to indicate the outcome of an operation.
*   The `Handler` translates internal errors (like `ErrOffsetNotFound`) into the appropriate protocol error codes.

## 10. Limitations (V1)

*   **Single Node:** No replication, no fault tolerance, no clustering.
*   **No Consumer Group Coordination:** While the broker now stores committed offsets per consumer group (`internal/offset`), it lacks the coordination protocol (like Kafka's group coordinator) needed to manage multiple consumer instances within the *same group*. Currently, if multiple consumers from the same group attempt to read the same partition, they will likely interfere with each other's offset commits, potentially leading to duplicate processing or skipped messages. The broker does not automatically assign partitions to consumers within a group or handle rebalancing on consumer failure/join/leave.
*   **Basic Retention Only:** Only size-based log retention (`MaxLogBytes`) is implemented. No time-based retention. No log compaction.
*   **No Security:** No authentication, authorization, or TLS encryption.
*   **No Transactions / Exactly-Once:** Delivery guarantee is effectively At-Least-Once (if a producer retries after a possible failure) or At-Most-Once (if producer doesn't retry).
*   **Limited Protocol:** Only basic Produce/Consume. No metadata requests, offset commits, etc.
*   **Simple Recovery:** Basic recovery relies on index file size. More complex scenarios (e.g., crash between log write and index write) could lead to minor data loss or inconsistency.
*   **Minimal Testing:** Lacks comprehensive unit and integration tests.

## 11. Future Directions

*   Introduce a mechanism (likely involving the broker as a coordinator or dedicated client roles) to manage consumer group membership, assign partitions exclusively to consumers within a group, and handle rebalancing when the group membership changes.
*   Add comprehensive Unit and Integration Tests.
*   Implement Metadata APIs (List Topics, Partitions, etc.).
*   Add Time-Based Log Retention.
*   Explore performance optimizations (e.g., buffer pooling with `sync.Pool`).
*   (Longer Term) Investigate replication strategies (e.g., Primary-Backup or Raft-based).
*   (Longer Term) Implement Consumer Group coordination.