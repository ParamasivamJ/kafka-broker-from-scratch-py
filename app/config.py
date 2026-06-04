"""
Broker configuration module.

Centralizes all configurable settings (paths, ports, etc.) so that the broker
can run in different environments without touching any other source file.

Environment variables take precedence over defaults.
"""

import os

# ---------------------------------------------------------------------------
# Data directory
# ---------------------------------------------------------------------------
# Locally:          ./data/kraft-combined-logs   (relative to project root)
# Override:         set KAFKA_LOG_DIR env var
# ---------------------------------------------------------------------------
DEFAULT_LOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "kraft-combined-logs",
)

LOG_DIR = os.environ.get("KAFKA_LOG_DIR", DEFAULT_LOG_DIR)

# ---------------------------------------------------------------------------
# Cluster metadata path (KRaft)
# ---------------------------------------------------------------------------
METADATA_PATH = os.path.join(
    LOG_DIR, "__cluster_metadata-0", "00000000000000000000.log",
)

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------
BROKER_HOST = os.environ.get("KAFKA_BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("KAFKA_BROKER_PORT", "9092"))


def get_partition_log_dir(topic_name: str, partition_index: int) -> str:
    """Return the directory path for a given topic/partition log."""
    return os.path.join(LOG_DIR, f"{topic_name}-{partition_index}")


def get_partition_log_path(topic_name: str, partition_index: int) -> str:
    """Return the full file path for a given topic/partition log."""
    return os.path.join(
        get_partition_log_dir(topic_name, partition_index),
        "00000000000000000000.log",
    )
