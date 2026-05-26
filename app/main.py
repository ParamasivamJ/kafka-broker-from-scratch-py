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
)
from metadata_utils import load_metadata, topic_uuid_exists


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

                    try:
                        metadata = load_metadata()
                        exists = topic_uuid_exists(
                            metadata, topic_id
                        )
                    except Exception as meta_err:
                        print("METADATA READ ERROR:", meta_err)
                        exists = False

                    if exists:
                        # CM4: topic exists, no messages
                        response = build_fetch_response_empty_records(
                            correlation_id,
                            topic_id
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