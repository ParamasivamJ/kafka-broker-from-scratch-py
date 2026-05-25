import sys
import os
import socket
import threading
sys.path.append(os.path.dirname(__file__))
from parsing import parse_topics
from protocol import (
    build_apiversions_response,
    build_describe_topic_partitions_response,
)


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