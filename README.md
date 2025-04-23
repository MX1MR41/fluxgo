# FluxGo

**A Lightweight, High-Performance Message Broker in Go**

FluxGo is a message broker and persistent log system inspired by Apache Kafka, built entirely in Go. It aims to provide core message queuing functionalities leveraging Go's strengths in concurrency, performance, and simplicity.

This project serves as a learning exercise in building distributed system components, focusing on the core principles of log-based message brokers.

## Introduction

FluxGo implements a publish/subscribe model based on a persistent, append-only commit log. Producers send messages to named topics (which are divided into partitions), and consumers read messages sequentially from these partitions, tracking their progress using offsets. Unlike traditional message queues, messages are not typically deleted upon consumption but are retained based on configurable policies (currently size-based).

**Goals:**

*   Implement core Kafka-like messaging primitives.
*   Leverage Go's concurrency (goroutines) for handling many connections efficiently.
*   Achieve high throughput and low latency through efficient I/O and minimal overhead.
*   Provide a simplified, understandable codebase for educational purposes.
*   Create a single, easily deployable binary.

## Core Concepts

*   **Broker:** The server process that handles client connections, manages topics/partitions, and stores messages.
*   **Topic:** A named category or feed to which messages are published.
*   **Partition:** A topic is divided into one or more partitions. Each partition is an ordered, immutable sequence of messages (a log). Partitions allow for parallelism and scalability. Ordering is only guaranteed *within* a partition.
*   **Offset:** A unique, sequential ID assigned to each message within a partition. Consumers use offsets to track their read position.
*   **Producer:** A client application that publishes messages to a specific topic (and optionally, partition).
*   **Consumer:** A client application that subscribes to one or more topics and reads messages sequentially from partitions, starting at a specific offset.
*   **Commit Log:** The underlying storage mechanism for each partition. Messages are appended to segment files on disk, providing durability.

## Current Features (V1 - In Progress)

*   **TCP Broker Server:** Listens for client connections.
*   **Topics & Partitions:** Managed implicitly via directory structure (`<data-dir>/<topic>_<partition>`). Logs are created/loaded on demand.
*   **Persistent Commit Log:**
    *   Append-only log files (`.log`).
    *   Log segmentation based on size (`max_segment_bytes`).
    *   Basic Indexing (`.index` files mapping relative offset to log file position).
*   **Produce API:** Clients can send messages to a specific topic and partition. The broker returns the assigned offset.
*   **Consume API:** Clients can request a message from a specific topic, partition, and offset.
*   **Consumer Offset Management:** Server-side storage and retrieval of consumer group offsets per partition (Commit/Fetch API). Stored in `<data-dir>/__consumer_offsets`.
*   **Configuration:** Server behavior configured via a YAML file (`configs/server.yaml`).
    *   Listen address, timeouts.
    *   Data directory, segment size, file sync behavior.
    *   Basic log retention based on total log size (`max_log_bytes`).
*   **Basic Command-Line Client:** Example client for producing, consuming, committing, and fetching offsets.

## Architecture Overview

FluxGo follows a layered architecture:

1.  **Client:** Interacts with the broker over TCP using a simple binary protocol (e.g., `cmd/fluxgo-client-example`).
2.  **Broker (`internal/broker`):** Handles TCP connections, manages connection lifecycle, and uses a handler for processing requests.
3.  **Handler (`internal/broker/handler.go`):** Parses incoming requests, dispatches them based on command codes, interacts with the `Store` and `OffsetManager`, and sends responses. Defines the request/response cycle logic.
4.  **Protocol (`internal/protocol`):** Defines the binary wire format for communication between clients and the broker, including command and error codes.
5.  **Store (`internal/store`):** Manages the collection of active commit logs, mapping topic/partition names to their respective `Log` instances. Handles loading logs from disk and creating new ones.
6.  **Offset Manager (`internal/offset`):** Manages persistent storage of consumer group offsets.
7.  **Commit Log (`internal/commitlog`):** The core persistence layer. Manages log segments (`.log` and `.index` files), handles appending records, reading by offset, segment rollovers, and basic retention.
8.  **Configuration (`internal/config`):** Loads and provides access to server settings from the YAML file.

The flow for a produce request looks roughly like:
`Client -> Broker (TCP) -> Handler -> Store (GetOrCreateLog) -> CommitLog (Append) -> Segment (Write + Index) -> Handler (Response) -> Client`

## Getting Started

### Prerequisites

*   **Go:** Version 1.18 or later installed ([https://go.dev/doc/install](https://go.dev/doc/install)).
*   **Git:** For cloning the repository (if applicable).

### Building

Navigate to the project's root directory in your terminal.

*   **Build Server:**
    *   Linux/macOS: `go build -o fluxgo-server cmd/fluxgo-server/main.go`
    *   Windows: `go build -o fluxgo-server.exe cmd\fluxgo-server\main.go`
*   **Build Client Example:**
    *   Linux/macOS: `go build -o fluxgo-client cmd/fluxgo-client-example/main.go`
    *   Windows: `go build -o fluxgo-client.exe cmd\fluxgo-client-example\main.go`

### Configuration

1.  Copy or rename `configs/server.yaml.example` to `configs/server.yaml` (or create your own).
2.  Edit `configs/server.yaml` to adjust settings:
    *   `server.listen_address`: Broker listen address (e.g., ":9898").
    *   `log.data_dir`: Base directory to store log data (e.g., "./fluxgo-data"). This also contains the `__consumer_offsets` subdirectory.
    *   `log.max_segment_bytes`: Max size for individual `.log` files before rolling over.
    *   `log.max_log_bytes`: Max total size for a single partition's log files before old segments are deleted.
    *   `log.file_sync`: Set to `true` to force fsync after writes (safer, potentially slower), `false` relies on OS caching (faster, less durable on crash).

### Running the Server

*   Ensure the configuration file exists at the path specified (or use the `-config` flag).
*   Linux/macOS: `./fluxgo-server -config configs/server.yaml`
*   Windows: `.\fluxgo-server.exe -config configs/server.yaml`

The server will start, create the data directory (including `__consumer_offsets`), load existing logs, and begin listening for connections.

### Running the Client Example

Open a separate terminal window. Use the `-group <group_id>` flag when committing or fetching offsets.

*   **Produce:**
    *   Linux/macOS: `./fluxgo-client -action produce -topic <topic_name> -partition <id> -message "Your Message"`
    *   Windows: `.\fluxgo-client.exe -action produce -topic <topic_name> -partition <id> -message "Your Message"`
    *(Example: `./fluxgo-client -action produce -topic orders -partition 0 -message "Order #123"`)*
*   **Consume:**
    *   Linux/macOS: `./fluxgo-client -action consume -topic <topic_name> -partition <id> -offset <offset_value>`
    *   Windows: `.\fluxgo-client.exe -action consume -topic <topic_name> -partition <id> -offset <offset_value>`
    *(Example: `./fluxgo-client -action consume -topic orders -partition 0 -offset 0`)*
*   **Commit Offset:** (Tell the server the next offset to read for this group)
    *   Linux/macOS: `./fluxgo-client -action commit -group <group_id> -topic <topic_name> -partition <id> -offset <next_offset>`
    *   Windows: `.\fluxgo-client.exe -action commit -group <group_id> -topic <topic_name> -partition <id> -offset <next_offset>`
    *(Example: Commit that offset 5 was processed: `./fluxgo-client -action commit -group my-consumers -topic orders -partition 0 -offset 6`)*
*   **Fetch Offset:** (Ask the server for the last committed offset for this group)
    *   Linux/macOS: `./fluxgo-client -action fetch -group <group_id> -topic <topic_name> -partition <id>`
    *   Windows: `.\fluxgo-client.exe -action fetch -group <group_id> -topic <topic_name> -partition <id>`
    *(Example: `./fluxgo-client -action fetch -group my-consumers -topic orders -partition 0`)*


See `-help` for more client options: `./fluxgo-client -help` or `.\fluxgo-client.exe -help`.

## Protocol (V1 - Basic Binary)

Communication happens over TCP using a simple length-prefixed binary protocol.

*   **Request Frame:**
    `[ 4-byte Length (N) ][ 1-byte Command Code ][ N-1 bytes Payload ]`
*   **Response Frame:**
    `[ 4-byte Length (M) ][ 1-byte Error Code ][ M-1 bytes Payload ]`

Lengths and multi-byte numeric fields use Big Endian encoding.

**Example Payloads:**

*   **Produce Request Payload:**
    `[ 2b TopicLen ][ Topic Name ][ 8b PartitionID ][ Message Data... ]`
*   **Produce Response Payload (Success):**
    `[ 8b Assigned Offset ]`
*   **Consume Request Payload:**
    `[ 2b TopicLen ][ Topic Name ][ 8b PartitionID ][ 8b Requested Offset ]`
*   **Consume Response Payload (Success):**
    `[ Message Data... ]`
*   **Commit Offset Request Payload:**
    `[ 2b GroupID Len ][ GroupID ][ 2b Topic Len ][ Topic ][ 8b PartitionID ][ 8b OffsetToCommit ]`
*   **Commit Offset Response Payload (Success):** (Empty)
*   **Fetch Offset Request Payload:**
    `[ 2b GroupID Len ][ GroupID ][ 2b Topic Len ][ Topic ][ 8b PartitionID ]`
*   **Fetch Offset Response Payload (Success):**
    `[ 8b Fetched Offset ]`

See `internal/protocol/codes.go` for command and error code definitions.

## Future Work / Roadmap

This is an ongoing project. Key areas for future development include:

*   **Consumer Group Coordination & Rebalancing:** Allow multiple consumers in the same group to dynamically share partitions.
*   **Testing:** Comprehensive unit and integration tests.
*   **Protocol Refinement:** More robust error handling, potential for batching.
*   **Metadata API:** Commands to list topics, partitions, get broker info.
*   **Advanced Retention:** Time-based log retention.
*   **(Long Term):** Replication, cluster coordination.

## Contributing

As this is primarily a personal learning project, contributions are not actively sought. However, feedback and suggestions are welcome via issues.

---
*Built with Go!* 🚀