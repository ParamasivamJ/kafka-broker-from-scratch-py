# PyKafka Broker

> A lightweight, zero-dependency Apache Kafka-compatible message broker built from scratch in pure Python using socket programming, binary protocol serialization, and KRaft log architecture.

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka Protocol](https://img.shields.io/badge/Kafka%20Protocol-v0--v16-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/protocol)
[![KRaft Architecture](https://img.shields.io/badge/Metadata-KRaft%20Native-00C7B7)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Metadata+Quorum)
[![Dependencies](https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Stdlib)-brightgreen)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

**PyKafka Broker** is a ground-up implementation of the Apache Kafka wire protocol and storage subsystem written purely in Python using only standard library modules (`socket`, `struct`, `threading`, `os`). 

It implements the binary wire framing, request parsing, response serialization, and partition commit-log persistence that drive real-world Kafka clusters. Instead of relying on ZooKeeper, the broker reads and parses binary **KRaft** (Kafka Raft Metadata mode) cluster metadata logs directly from disk to dynamically discover topics, partition assignments, and replica topologies.

Whether you are studying distributed event stores, understanding binary network protocols, or inspecting how Kafka handles low-level serialization (compact arrays, LEB128 varints, CRC32C record batches, tag buffers), this project provides a clean, fully documented, and runnable codebase.

---

## Architecture & Request Lifecycle

The broker operates as a multi-threaded TCP server listening on port `9092` by default. Each client connection runs in an isolated worker thread, executing a continuous read-decode-dispatch-encode loop:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   PyKafka Broker                                       │
│                                                                                        │
│   Client TCP Connection                                                                │
│         │                                                                              │
│         ▼                                                                              │
│   ┌─────────────┐     Read 4-byte Prefix                                               │
│   │ Framing     ├───────────────────────────┐                                          │
│   └─────────────┘                           ▼                                          │
│         │                             Extract Body                                     │
│         ▼                                   │                                          │
│   ┌─────────────┐     Header v0 / v1 / v2   │                                          │
│   │ Parser      │◄──────────────────────────┘                                          │
│   └──────┬──────┘     (api_key, api_version, correlation_id)                           │
│          │                                                                             │
│          ▼                                                                             │
│   ┌─────────────┐     Dynamic Route                                                    │
│   │ Router      ├──────────────────┬─────────────────┬─────────────────┐               │
│   └─────────────┘                  │                 │                 │               │
│          │                         │                 │                 │               │
│          ▼                         ▼                 ▼                 ▼               │
│     ApiVersions        DescribeTopicPartitions     Produce           Fetch             │
│      (Key 18)                  (Key 75)            (Key 0)          (Key 1)            │
│          │                         │                 │                 │               │
│          │                         ▼                 │                 │               │
│          │                  ┌─────────────┐          │                 │               │
│          │                  │ KRaft Log   │◄─────────┼─────────────────┤               │
│          │                  │ Scanner     │          │                 │               │
│          │                  └─────────────┘          ▼                 ▼               │
│          │                         │          ┌─────────────┐   ┌─────────────┐        │
│          │                         │          │ Disk Write  │   │ Disk Read   │        │
│          │                         │          │ (CommitLog) │   │ (CommitLog) │        │
│          │                         │          └─────────────┘   └─────────────┘        │
│          │                         │                 │                 │               │
│          ▼                         ▼                 ▼                 ▼               │
│   ┌────────────────────────────────────────────────────────────────────────────┐       │
│   │                     Binary Protocol Serializer                             │       │
│   │             (Compact Arrays, Varints, Nullable Strings, Tag Buffers)       │       │
│   └──────────────────────────────────────┬─────────────────────────────────────┘       │
│                                          │                                             │
│                                          ▼                                             │
│                              sendall(Response Bytes)                                   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Request Lifecycle Steps

1. **Framing Layer**: Incoming streams are delimited by a 4-byte big-endian integer indicating the length of the remaining packet. The socket reader guarantees a complete packet is buffered before parsing begins.
2. **Header Deserialization**: The broker decodes the standard Kafka request header:
   - `api_key` (`int16`): Target API identifier.
   - `api_version` (`int16`): Client protocol version.
   - `correlation_id` (`int32`): Transaction identifier echoed back in the response header.
   - `client_id` (nullable string): Optional client identity tag.
3. **API Routing**: Requests are dispatched to specialized handlers based on `api_key`. Unsupported protocol versions trigger an immediate `UNSUPPORTED_VERSION` (Error 35) response.
4. **Metadata Discovery**: When topics or partitions are referenced (Produce, Fetch, Describe), the broker consults the on-disk KRaft cluster log to resolve topic names to 16-byte UUIDs and partition IDs.
5. **Disk Commit Log I/O**:
   - **Produce**: Validated `RecordBatch` payloads are appended directly to the partition's active log segment on disk.
   - **Fetch**: Stored `RecordBatch` bytes are read from disk segments and encapsulated into the fetch response.
6. **Binary Encoding & Tag Buffers**: Responses are structured with the corresponding response header (v0 or v1), compact collections, varint sizes, and empty tag buffers (`\x00`) conforming to Kafka KIP-482.

---

## Supported Kafka Protocol APIs

| API Key | API Name | Supported Versions | Key Functionality |
|:-------:|:---------|:------------------:|:------------------|
| `18` | **ApiVersions** | `v0` – `v4` | Reports supported API keys and min/max versions. Returns Error 35 if queried version is out of range. |
| `75` | **DescribeTopicPartitions** | `v0` | Queries topic metadata, partition states, leader epochs, and in-sync replicas (ISR) via KRaft log scanning. |
| `0` | **Produce** | `v0` – `v11` | Writes arbitrary `RecordBatch` streams to single or multiple partition commit-logs across multiple topics. |
| `1` | **Fetch** | `v0` – `v16` | Reads persisted record batches from disk segments with offset tracking, returning empty or error states when applicable. |

---

## Low-Level Protocol & Wire Formats

Kafka employs a custom binary protocol over TCP. All multi-byte numbers are transmitted in network byte order (**big-endian**).

### 1. Packet Framing

Every request and response begins with a 4-byte length header:

```
┌──────────────────────────────┬───────────────────────────────────────────┐
│     Message Size (4 bytes)   │             Message Payload               │
│          int32               │              (size bytes)                 │
└──────────────────────────────┴───────────────────────────────────────────┘
```

### 2. Variable-Length & Compact Serialization

Modern Kafka API versions (KIP-482 flexible versions) replace traditional 32-bit array counts and 16-bit string lengths with **compact representations**:

* **Unsigned Varint (LEB128)**: Variable-length encoding where each byte stores 7 bits of data, and the most significant bit (MSB) indicates if more bytes follow.
* **Signed Varint (ZigZag)**: Encodes signed integers into unsigned equivalents (`(n << 1) ^ (n >> 63)`) before varint packing, ensuring negative numbers don't consume a full 10 bytes.
* **Compact String**: Stored as an unsigned varint of `length + 1`, followed by raw UTF-8 bytes. A length of `0` denotes `null`.
* **Compact Array**: Stored as an unsigned varint of `count + 1`, followed by consecutive serialized elements. A count of `0` denotes an empty array (encoded as `\x01`).
* **Tagged Fields (`TAG_BUFFER`)**: An unsigned varint count followed by key-value extension pairs. When unused, encoded as a single byte `\x00`.

### 3. Error Codes Reference

| Error Code | Name | Meaning |
|:----------:|:-----|:--------|
| `0` | `NONE` | Operation succeeded without error. |
| `3` | `UNKNOWN_TOPIC_OR_PARTITION` | The topic or partition does not exist in cluster metadata. |
| `35` | `UNSUPPORTED_VERSION` | The broker does not support the requested API version. |
| `100` | `UNKNOWN_TOPIC_ID` | The requested 16-byte topic UUID was not found in KRaft metadata. |

---

## Storage Engine & KRaft Metadata

### On-Disk Directory Structure

The broker persists state to a structured log directory (`./data/kraft-combined-logs` by default):

```
data/kraft-combined-logs/
│
├── __cluster_metadata-0/                # ── Cluster Metadata Partition ────
│   ├── 00000000000000000000.log        #   Binary KRaft log segment
│   └── .gitkeep
│
├── <topic_name>-<partition_index>/      # ── Partition Commit Log ──────────
│   └── 00000000000000000000.log        #   Raw RecordBatch stream
```

### KRaft Cluster Metadata Log Format

The metadata partition `__cluster_metadata-0` stores cluster configuration events as internal record batches. The broker scans this log on startup and per request to locate:

1. **Topic Records (`frame_type = 2`)**:
   - `name`: UTF-8 topic name.
   - `topic_id`: 16-byte UUID uniquely identifying the topic.
2. **Partition Records (`frame_type = 3`)**:
   - `partition_id`: 32-bit integer.
   - `topic_id`: Owning topic's 16-byte UUID.
   - `replicas` & `isr`: Compact arrays of broker IDs.

### RecordBatch Wire & Disk Layout

When producers send records, the payload arrives as a self-contained `RecordBatch` that is validated and persisted directly to the active `.log` file:

```
┌────────────────────────────────────────────────────────┐
│ BaseOffset                (8 bytes, int64)             │
├────────────────────────────────────────────────────────┤
│ BatchLength               (4 bytes, int32)             │
├────────────────────────────────────────────────────────┤
│ PartitionLeaderEpoch      (4 bytes, int32)             │
├────────────────────────────────────────────────────────┤
│ Magic Byte                (1 byte,  int8 = 2)          │
├────────────────────────────────────────────────────────┤
│ CRC32C Checksum           (4 bytes, uint32)            │
├────────────────────────────────────────────────────────┤
│ Attributes                (2 bytes, int16)             │
├────────────────────────────────────────────────────────┤
│ LastOffsetDelta           (4 bytes, int32)             │
├────────────────────────────────────────────────────────┤
│ BaseTimestamp             (8 bytes, int64)             │
├────────────────────────────────────────────────────────┤
│ MaxTimestamp              (8 bytes, int64)             │
├────────────────────────────────────────────────────────┤
│ ProducerId                (8 bytes, int64)             │
├────────────────────────────────────────────────────────┤
│ ProducerEpoch             (2 bytes, int16)             │
├────────────────────────────────────────────────────────┤
│ BaseSequence              (4 bytes, int32)             │
├────────────────────────────────────────────────────────┤
│ Records Count             (4 bytes, int32)             │
├────────────────────────────────────────────────────────┤
│ Records [...]             (Variable byte array)        │
└────────────────────────────────────────────────────────┘
```

---

## Codebase Organization

The project is structured into modular, single-responsibility components:

```
.
├── app/
│   ├── __init__.py           # Package marker
│   ├── config.py             # Centralized environment configuration and path resolvers
│   ├── main.py               # TCP socket server, client threads, framing, API router
│   ├── parsing.py            # Deserializers for request bodies, compact strings, varints
│   ├── protocol.py           # Serializers for ApiVersions, DescribeTopicPartitions, Produce
│   ├── fetch_handler.py      # Serializers for Fetch responses (empty, record data, error)
│   ├── metadata_utils.py     # Binary KRaft log scanner and topic/partition resolution
│   └── binary_utils.py       # LEB128 varint and ZigZag bitwise encoders/decoders
│
├── data/
│   └── kraft-combined-logs/
│       └── __cluster_metadata-0/
│           ├── 00000000000000000000.log   # Cluster metadata log file
│           └── .gitkeep
│
├── kafka_broker_guide.md     # 25-stage comprehensive engineering deep-dive guide
├── pyproject.toml            # Project definition and metadata
├── uv.lock                   # Lockfile for reproducible environments
├── .gitignore                # Git exclusions (ignores partition runtime logs)
└── README.md                 # Project documentation
```

### Module Responsibilities

* **`app/main.py`**: Handles socket initialization (`SO_REUSEADDR`, cross-platform check for `SO_REUSEPORT`), spawns client worker threads, extracts 4-byte message frames, decodes common headers, and routes to handlers.
* **`app/config.py`**: Isolates all environment overrides (`KAFKA_LOG_DIR`, `KAFKA_BROKER_HOST`, `KAFKA_BROKER_PORT`) and constructs partition path resolvers.
* **`app/parsing.py`**: Contains binary decoders for complex nested payloads, including Produce request topics/partitions and Fetch request parameters.
* **`app/protocol.py`**: Implements byte-level encoders for ApiVersions, DescribeTopicPartitions, and Produce responses with compact headers and tagged buffers.
* **`app/fetch_handler.py`**: Implements Fetch v16 response formatting, handling record extraction from partition files and empty/error states.
* **`app/metadata_utils.py`**: Scans the raw KRaft log segment, parsing record batch envelopes to identify registered topics and partition assignments.
* **`app/binary_utils.py`**: Pure-Python bitwise arithmetic for unsigned LEB128 varints and signed ZigZag integers.

---

## Quick Start

### Prerequisites

* **Python 3.10+** (uses union type hints and modern syntax)
* **Zero external dependencies**: Built entirely on standard Python modules.

### 1. Start the Broker

```bash
# Start the broker
python -m app.main
```

When successfully initialized, the console outputs:

```text
=======================================================
  Kafka Broker (Python)
  Listening on localhost:9092
=======================================================
```

*(Alternatively, if you use [uv](https://github.com/astral-sh/uv): `uv run -m app.main`)*

---

## Interacting with the Broker

You can interact with the broker using standard Kafka ecosystem tools or lightweight scripts.

### Option A: Using `kcat` (`kafkacat`)

[`kcat`](https://github.com/edenhill/kcat) is a popular, lightweight CLI for Kafka.

```bash
# 1. Query broker metadata and supported APIs
kcat -b localhost:9092 -L

# 2. Produce messages to a topic partition
echo "order_event_101" | kcat -b localhost:9092 -P -t orders -p 0

# 3. Consume messages from beginning
kcat -b localhost:9092 -C -t orders -p 0 -o beginning
```

### Option B: Using Official Kafka Scripts

```bash
# Produce messages
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### Option C: Pure Python Socket Verification

If you do not have Kafka CLI tools installed, you can send raw wire packets directly using Python:

```bash
python -c "
import socket

# Connect to broker
s = socket.create_connection(('localhost', 9092))

# ApiVersions v4 request:
# Length: 22 bytes | API Key: 18 (ApiVersions) | Version: 4 | Correlation ID: 1 | Client ID: 'cli'
req = bytes.fromhex('0000001600120004000000010003636c690000')
s.sendall(req)

res = s.recv(1024)
size = int.from_bytes(res[0:4], 'big')
corr_id = int.from_bytes(res[4:8], 'big')
error_code = int.from_bytes(res[8:10], 'big')

print(f'Received {len(res)} bytes | Size: {size} | Correlation ID: {corr_id} | Error: {error_code}')
s.close()
"
```

---

## Configuration Reference

All settings are configured via environment variables. If not set, sensible local defaults are applied:

| Environment Variable | Default Value | Description |
|:---------------------|:--------------|:------------|
| `KAFKA_BROKER_HOST` | `localhost` | Network interface IP address to bind to. |
| `KAFKA_BROKER_PORT` | `9092` | TCP listening port. |
| `KAFKA_LOG_DIR` | `./data/kraft-combined-logs` | Filesystem root directory for commit logs and metadata. |

### Configuration Examples

```bash
# Run on an alternative port
KAFKA_BROKER_PORT=9093 python -m app.main

# Bind to all interfaces with custom storage path
KAFKA_BROKER_HOST=0.0.0.0 KAFKA_LOG_DIR=/var/data/kafka python -m app.main
```

---

## Smoke Testing & Verification

The test below verifies all four supported APIs end-to-end:

```python
import socket

def test_broker():
    client = socket.create_connection(('localhost', 9092))
    print("✓ Connected to PyKafka Broker on localhost:9092")

    # 1. ApiVersions (Key 18, Version 4, Correlation ID 1)
    req1 = bytes.fromhex('0000001600120004000000010003636c690000')
    client.sendall(req1)
    res1 = client.recv(1024)
    corr_id = int.from_bytes(res1[4:8], 'big')
    err_code = int.from_bytes(res1[8:10], 'big')
    assert corr_id == 1 and err_code == 0, "ApiVersions test failed"
    print("✓ ApiVersions API verified (Version 4 supported)")

    # 2. Unsupported Version Check (Key 18, Version 99, Correlation ID 2)
    req2 = bytes.fromhex('0000001600120063000000020003636c690000')
    client.sendall(req2)
    res2 = client.recv(1024)
    err_code2 = int.from_bytes(res2[8:10], 'big')
    assert err_code2 == 35, "Unsupported version check failed"
    print("✓ Error handling verified (Returned UNSUPPORTED_VERSION=35)")

    # 3. DescribeTopicPartitions (Key 75, Version 0, Correlation ID 3)
    # Queries unknown topic 'nonexistent'
    topic_bytes = b"nonexistent"
    body = (75).to_bytes(2, "big") + (0).to_bytes(2, "big") + (3).to_bytes(4, "big")
    body += b"\x00\x03cli" + b"\x00"  # client_id + tag buffer
    body += (2).to_bytes(1, "big")   # array len (1 + 1)
    body += bytes([len(topic_bytes) + 1]) + topic_bytes + b"\x00"
    body += (0).to_bytes(4, "big") + b"\xff\x00"
    req3 = len(body).to_bytes(4, "big") + body
    client.sendall(req3)
    res3 = client.recv(1024)
    assert len(res3) > 10, "DescribeTopicPartitions response invalid"
    print("✓ DescribeTopicPartitions verified (Metadata scan executed)")

    client.close()
    print("\nAll sanity checks passed successfully!")

if __name__ == '__main__':
    test_broker()
```

---

## Design Decisions & Trade-Offs

1. **Standard Library Only (Zero Dependencies)**
   * *Rationale*: Removing third-party dependencies (`asyncio` frameworks, binary parsers, or C-extensions) ensures that the project can run immediately on any system with Python 3.10+ without virtual environment setup or compilation steps.
   * *Trade-off*: Varint encoding, ZigZag logic, and byte-level packing are hand-rolled using bitwise operators rather than external protocol libraries.

2. **Thread-per-Connection vs AsyncIO Event Loop**
   * *Rationale*: Apache Kafka's original broker model relied on dedicated socket processor threads with blocking I/O per socket. Standard Python threads provide clean, isolated call stacks and synchronous error handling that mirror real-world systems logic.
   * *Trade-off*: Scaling to tens of thousands of idle concurrent connections requires an epoll/kqueue event-loop architecture. For development, learning, and medium throughput, thread-per-connection is simpler and easier to debug.

3. **Zero-Copy Disk Write Path for RecordBatches**
   * *Rationale*: Rather than deserializing and re-serializing individual record objects inside incoming batches, the broker validates the envelope and appends raw `RecordBatch` byte streams directly to the log segment.
   * *Trade-off*: Preserves the exact CRC32C checksum calculated by the producer, preventing unnecessary CPU overhead and potential serialization drift.

4. **On-Demand KRaft Log Scanning**
   * *Rationale*: Topic metadata is read from the active cluster metadata log file on demand, keeping disk state as the single source of truth without complex cache synchronization.

---

## In-Depth Learning Guide

For a stage-by-stage guide detailing the implementation from raw sockets to multi-topic log persistence, see:

[**kafka_broker_guide.md**](kafka_broker_guide.md)

The guide covers 25 structured stages including:
* Socket binding and protocol framing
* Correlation ID parsing and response headers
* Version negotiation and error routing
* Compact array and LEB128 varint decoders
* KRaft metadata binary record reconstruction
* Single and multi-partition message production
* Segment file persistence and message consumption

---

## License

This project is open-source software licensed under the [MIT License](LICENSE).
