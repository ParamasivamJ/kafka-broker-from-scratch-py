# Kafka Broker — Python

A **fully functional Kafka-compatible message broker** built from scratch in Python using only the standard library. Implements the Kafka binary wire protocol including `ApiVersions`, `DescribeTopicPartitions`, `Fetch`, and `Produce` APIs.

---

## Project Structure

```
kafka-broker-python/
│
├── app/                              # ── Broker source code ──────────────
│   ├── __init__.py                   #   Package marker
│   ├── config.py                     #   Centralized configuration (paths, ports)
│   ├── main.py                       #   TCP server, request router, API handlers
│   ├── parsing.py                    #   Binary request deserializers
│   ├── protocol.py                   #   Response serializers (ApiVersions, DTP, Produce)
│   ├── fetch_handler.py              #   Response serializers (Fetch variants)
│   ├── metadata_utils.py             #   KRaft metadata log scanner
│   └── binary_utils.py              #   Shared varint encode/decode utilities
│
├── data/                             # ── Runtime data (local dev) ────────
│   └── kraft-combined-logs/
│       └── __cluster_metadata-0/
│           ├── 00000000000000000000.log   # KRaft metadata (empty by default)
│           └── .gitkeep
│
├── kafka_broker_guide.md             #  Complete learning guide 
├── pyproject.toml                    # Python project metadata
├── uv.lock                          # Dependency lockfile
├── .gitignore                        # Git exclusions (runtime logs ignored)
└── README.md                         # ← You are here
```

---

## Quick Start (Local Development)

### Prerequisites

- **Python 3.10+** (uses `match` style `|` union types)
- No external dependencies — everything is built with the Python standard library

### Run the broker

```bash
# Option 1: Direct Python
python -m app.main

# Option 2: Using uv (if installed)
uv run --quiet -m app.main
```

You'll see:

```
=======================================================
  Kafka Broker (Python)
  Listening on localhost:9092
=======================================================
```

### Connect to it

Use any Kafka client library, or a tool like [`kcat`](https://github.com/edenhill/kcat):

```bash
# Check API versions
kcat -b localhost:9092 -L

# Produce a message
echo "hello" | kcat -b localhost:9092 -P -t my-topic

# Consume messages
kcat -b localhost:9092 -C -t my-topic
```

---

## Configuration

All configuration is centralized in [`app/config.py`](app/config.py) and driven by **environment variables**:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_LOG_DIR` | `./data/kraft-combined-logs` | Base directory for all log files |
| `KAFKA_BROKER_HOST` | `localhost` | Broker bind address |
| `KAFKA_BROKER_PORT` | `9092` | Broker listen port |

**Example — custom port:**

```bash
KAFKA_BROKER_PORT=19092 python -m app.main
```

---

## Learning Guide

See [`kafka_broker_guide.md`](kafka_broker_guide.md) for a comprehensive, walkthrough covering:

- TCP socket programming and the Kafka binary protocol
- Compact arrays, varints, and compact strings
- KRaft metadata log scanning
- RecordBatch persistence and retrieval
- Concurrent client handling with threads
- Complete byte-level diagrams and annotated code

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    TCP Server (main.py)                  │
│                                                         │
│   Client ──► recv() ──► Parse Header ──► Route by Key   │
│                                              │          │
│              ┌───────────┬───────────┬───────┴────┐     │
│              ▼           ▼           ▼            ▼     │
│         ApiVersions   Describe    Produce      Fetch    │
│        (key=18)      (key=75)    (key=0)      (key=1)   │
│              │           │           │            │     │
│              │      ┌────┴────┐  ┌───┴───┐  ┌────┴───┐ │
│              │      │Metadata │  │ Write  │  │ Read   │ │
│              │      │ Utils   │  │ Disk   │  │ Disk   │ │
│              │      └─────────┘  └───────┘  └────────┘ │
│              ▼           ▼           ▼            ▼     │
│           sendall() ◄── Serialize Response              │
└─────────────────────────────────────────────────────────┘
```

### Supported APIs

| API Key | Name | Version Range | Description |
|---------|------|--------------|-------------|
| `0` | Produce | v0–v11 | Write records to topic partitions |
| `1` | Fetch | v0–v16 | Read records from topic partitions |
| `18` | ApiVersions | v0–v4 | Discover supported APIs |
| `75` | DescribeTopicPartitions | v0 | List topic metadata and partitions |

---

## Testing

### Local smoke test

```bash
# Terminal 1: Start the broker
python -m app.main

# Terminal 2: Send a raw ApiVersions request (Python one-liner)
python -c "
import socket
s = socket.create_connection(('localhost', 9092))
# ApiVersions v4 request: msg_size=22, api_key=18, api_version=4, corr_id=1
request = bytes.fromhex('00000016001200040000000100096b61666b612d636c69000')
s.sendall(request)
response = s.recv(1024)
print('Response received:', len(response), 'bytes')
print('Correlation ID:', int.from_bytes(response[4:8], 'big'))
s.close()
"
```

---

