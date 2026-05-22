import socket
import threading


def handle_client(conn):

    while True:

        try:
            # Receive request
            request = conn.recv(1024)

            # Client disconnected
            if not request:
                break

            # -------------------------
            # Parse request
            # -------------------------

            api_version = int.from_bytes(
                request[6:8],
                byteorder="big"
            )

            correlation_id = request[8:12]

            # -------------------------
            # Determine error code
            # -------------------------

            if 0 <= api_version <= 4:
                error_code = 0
            else:
                error_code = 35

            # -------------------------
            # Build response body
            # -------------------------

            response_body = b""

            # error_code
            response_body += error_code.to_bytes(2, "big")

            # COMPACT_ARRAY with 1 entry
            response_body += b"\x02"

            # ApiVersions entry
            response_body += (18).to_bytes(2, "big")
            response_body += (0).to_bytes(2, "big")
            response_body += (4).to_bytes(2, "big")

            # tag buffer
            response_body += b"\x00"

            # throttle_time_ms
            response_body += (0).to_bytes(4, "big")

            # final tag buffer
            response_body += b"\x00"

            # -------------------------
            # Header
            # -------------------------

            response_header = correlation_id

            message_size = (
                len(response_header) +
                len(response_body)
            )

            response = (
                message_size.to_bytes(4, "big") +
                response_header +
                response_body
            )

            # Send response
            conn.sendall(response)

        except Exception as e:
            print(f"Error: {e}")
            break

    conn.close()


def main():

    print("Kafka broker starting...")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    while True:

        # Accept new client
        conn, addr = server.accept()

        print(f"Connection from {addr}")

        # Create thread for client
        thread = threading.Thread(
            target=handle_client,
            args=(conn,)
        )

        thread.start()


if __name__ == "__main__":
    main()