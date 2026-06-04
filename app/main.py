"""
Kafka broker — main entry point.

Listens on a TCP socket, accepts client connections (one thread each),
and routes requests to the appropriate API handler based on api_key.
"""

import os
import socket
import sys
import threading

sys.path.append(os.path.dirname(__file__))

from config import BROKER_HOST, BROKER_PORT, get_partition_log_dir, get_partition_log_path
from parsing import parse_topics, parse_fetch_request, parse_produce_request
from protocol import (
    build_apiversions_response,
    build_describe_topic_partitions_response,
    build_produce_response,
)
from fetch_handler import (
    build_fetch_response_empty_topics,
    build_fetch_unknown_topic_response,
    build_fetch_response_empty_records,
    build_fetch_response_with_records,
)
from metadata_utils import (
    load_metadata,
    topic_uuid_exists,
    find_topic_name_by_uuid,
    find_topic_metadata,
    extract_partitions,
)


# ── API Keys ────────────────────────────────────────────────────────────────
API_PRODUCE = 0
API_FETCH = 1
API_VERSIONS = 18
API_DESCRIBE_TOPIC_PARTITIONS = 75


# ── Request Handlers ────────────────────────────────────────────────────────

def handle_api_versions(correlation_id, api_version):
    """Handle ApiVersions (key 18) requests."""
    return build_apiversions_response(correlation_id, api_version)


def handle_describe_topic_partitions(correlation_id, request):
    """Handle DescribeTopicPartitions (key 75) requests."""
    topics = parse_topics(request)
    return build_describe_topic_partitions_response(correlation_id, topics)


def handle_produce(correlation_id, api_version, request):
    """Handle Produce (key 0) requests — validate, persist, respond."""
    print("\n========== PRODUCE REQUEST ==========")
    print(f"PRODUCE API VERSION: {api_version}")

    produce_data = parse_produce_request(request)
    metadata = load_metadata()
    topics_response_data = []

    for topic in produce_data["topics"]:
        topic_name = topic["name"]
        topic_name_bytes = topic_name.encode("utf-8")

        # ── Validate topic ──────────────────────────────────────────────
        try:
            topic_metadata = find_topic_metadata(metadata, topic_name_bytes)
        except Exception as topic_err:
            print("Error finding topic metadata:", topic_err)
            topic_metadata = None

        topic_uuid = topic_metadata["uuid"] if topic_metadata else None

        try:
            partitions = extract_partitions(metadata, topic_uuid) if topic_uuid else []
        except Exception as part_err:
            print("Error extracting partitions:", part_err)
            partitions = []

        # ── Process each partition ──────────────────────────────────────
        partition_responses = []
        for part in topic["partitions"]:
            part_index = part["index"]
            records = part.get("records", b"")

            if topic_uuid and (part_index in partitions):
                error_code = 0
                # Persist RecordBatch to disk
                if records:
                    try:
                        log_dir = get_partition_log_dir(topic_name, part_index)
                        os.makedirs(log_dir, exist_ok=True)
                        log_file_path = get_partition_log_path(topic_name, part_index)
                        with open(log_file_path, "ab") as f:
                            f.write(records)
                        print(f"Successfully persisted {len(records)} bytes to {log_file_path}")
                    except Exception as persist_err:
                        print("Error persisting records:", persist_err)
            else:
                error_code = 3  # UNKNOWN_TOPIC_OR_PARTITION

            partition_responses.append({
                "index": part_index,
                "error_code": error_code,
            })

        topics_response_data.append({
            "name": topic_name,
            "partitions": partition_responses,
        })

    return build_produce_response(correlation_id, topics_response_data)


def handle_fetch(correlation_id, api_version, request):
    """Handle Fetch (key 1) requests — read records from disk."""
    print("\n========== FETCH REQUEST ==========")
    print(f"FETCH API VERSION: {api_version}")

    fetch_data = parse_fetch_request(request)

    # ── Empty topics ────────────────────────────────────────────────────
    if fetch_data["topics_count"] == 0:
        return build_fetch_response_empty_topics(correlation_id)

    # ── Route: known vs unknown topic ───────────────────────────────────
    topic_id = fetch_data["topic_id"]
    partition_index = fetch_data.get("partition_index", 0)

    try:
        metadata = load_metadata()
        exists = topic_uuid_exists(metadata, topic_id)
    except Exception as meta_err:
        print("METADATA READ ERROR:", meta_err)
        exists = False

    if not exists:
        return build_fetch_unknown_topic_response(correlation_id, topic_id)

    # ── Topic exists — check for records on disk ────────────────────────
    topic_name = find_topic_name_by_uuid(metadata, topic_id)
    record_bytes = b""
    if topic_name:
        log_file_path = get_partition_log_path(topic_name, partition_index)
        if os.path.exists(log_file_path):
            try:
                with open(log_file_path, "rb") as f:
                    record_bytes = f.read()
                print(f"READ {len(record_bytes)} BYTES FROM DISK FOR PARTITION {partition_index}")
            except Exception as log_err:
                print("LOG FILE READ ERROR:", log_err)

    if record_bytes:
        return build_fetch_response_with_records(
            correlation_id, topic_id, partition_index, record_bytes,
        )
    else:
        return build_fetch_response_empty_records(
            correlation_id, topic_id, partition_index,
        )


# ── Client Connection Loop ──────────────────────────────────────────────────

def handle_client(conn):
    """Read requests in a loop from a single client connection."""
    while True:
        try:
            request = conn.recv(1024)

            if not request:
                break

            # ── Parse common request header ─────────────────────────────
            api_key = int.from_bytes(request[4:6], "big")
            api_version = int.from_bytes(request[6:8], "big")
            correlation_id = request[8:12]

            # ── Route to handler ────────────────────────────────────────
            if api_key == API_VERSIONS:
                response = handle_api_versions(correlation_id, api_version)

            elif api_key == API_DESCRIBE_TOPIC_PARTITIONS:
                response = handle_describe_topic_partitions(correlation_id, request)

            elif api_key == API_PRODUCE:
                response = handle_produce(correlation_id, api_version, request)

            elif api_key == API_FETCH:
                response = handle_fetch(correlation_id, api_version, request)

            else:
                print(f"Unknown API key: {api_key}")
                continue

            conn.sendall(response)

        except Exception as e:
            print(f"Error: {e}")
            break

    conn.close()


# ── Server Bootstrap ────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Kafka Broker (Python)")
    print(f"  Listening on {BROKER_HOST}:{BROKER_PORT}")
    print("=" * 55)

    server = socket.create_server(
        (BROKER_HOST, BROKER_PORT),
        reuse_port=(sys.platform != "win32"),
    )

    while True:
        conn, addr = server.accept()
        print(f"Connection from {addr}")

        thread = threading.Thread(
            target=handle_client,
            args=(conn,),
        )
        thread.start()


if __name__ == "__main__":
    main()