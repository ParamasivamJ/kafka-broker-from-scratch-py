import sys
import os
import socket
import threading
sys.path.append(os.path.dirname(__file__))
from parsing import parse_topics, parse_fetch_request
from protocol import (
    build_apiversions_response,
    build_describe_topic_partitions_response,
)
from fetch_handler import (
    build_fetch_response_empty_topics,
    build_fetch_unknown_topic_response,
    build_fetch_response_empty_records,
    build_fetch_response_with_records,
)
from metadata_utils import load_metadata, topic_uuid_exists, find_topic_name_by_uuid


def handle_client(conn):
    while True:
        try:
            request = conn.recv(1024)

            if not request:
                break

            # Common header
            api_key = int.from_bytes(request[4:6], "big")
            api_version = int.from_bytes(request[6:8], "big")
            correlation_id = request[8:12]

            if api_key == 18:
                response = build_apiversions_response(
                    correlation_id,
                    api_version,
                )
                conn.sendall(response)

            elif api_key == 75:
                topics = parse_topics(request)
                response = build_describe_topic_partitions_response(
                    correlation_id,
                    topics,
                )
                conn.sendall(response)
            
            elif api_key == 1:

                print("\n========== FETCH REQUEST ==========")

                print(
                    f"FETCH API VERSION: "
                    f"{api_version}"
                )

                fetch_data = parse_fetch_request(
                    request
                )

                # -------------------------------------------------
                # EMPTY TOPICS
                # -------------------------------------------------

                if fetch_data["topics_count"] == 0:

                    response = (
                        build_fetch_response_empty_topics(
                            correlation_id
                        )
                    )

                # -------------------------------------------------
                # Route: known topic (empty records) vs unknown topic
                # -------------------------------------------------

                else:

                    topic_id = fetch_data["topic_id"]
                    partition_index = fetch_data.get("partition_index", 0)

                    try:
                        metadata = load_metadata()
                        exists = topic_uuid_exists(
                            metadata, topic_id
                        )
                    except Exception as meta_err:
                        print("METADATA READ ERROR:", meta_err)
                        exists = False

                    if exists:
                        # CM4 / EG2 / FD8: topic exists. Check if there are messages on disk.
                        topic_name = find_topic_name_by_uuid(metadata, topic_id)
                        record_bytes = b""
                        if topic_name:
                            log_dir = f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}"
                            log_file_path = f"{log_dir}/00000000000000000000.log"
                            if os.path.exists(log_file_path):
                                try:
                                    with open(log_file_path, "rb") as f:
                                        record_bytes = f.read()
                                    print(f"READ {len(record_bytes)} BYTES FROM DISK FOR PARTITION {partition_index}")
                                except Exception as log_err:
                                    print("LOG FILE READ ERROR:", log_err)

                        if record_bytes:
                            # Stage EG2 / FD8: topic exists and has messages
                            response = build_fetch_response_with_records(
                                correlation_id,
                                topic_id,
                                partition_index,
                                record_bytes
                            )
                        else:
                            # Stage CM4: topic exists but has no messages
                            response = build_fetch_response_empty_records(
                                correlation_id,
                                topic_id,
                                partition_index
                            )
                    else:
                        # HN6: topic does not exist
                        response = build_fetch_unknown_topic_response(
                            correlation_id,
                            topic_id
                        )

                conn.sendall(response)

        except Exception as e:
            print(f"Error: {e}")
            break

    conn.close()


def main():
    print("Kafka broker starting...")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True,
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