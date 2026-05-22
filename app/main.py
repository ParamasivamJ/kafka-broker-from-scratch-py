import socket


def main():

    print("Kafka broker starting...")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    
    conn, addr = server.accept()

    print(f"Connection from {addr}")

    while True:
        # Receive request
        request = conn.recv(1024)

        # Client disconnected
        if not request:
            break

        # Extract fields
        api_version = int.from_bytes(
            request[6:8],
            byteorder="big"
        )

        correlation_id = request[8:12]

        # Determine error code
        if 0 <= api_version <= 4:
            error_code = 0
        else:
            error_code = 35

        # -------------------------
        # Build response body
        # -------------------------

        response_body = b""

        # error_code (INT16)
        response_body += error_code.to_bytes(2, "big")

        # api_keys COMPACT_ARRAY
        # 02 means array with 1 element
        response_body += b"\x02"

        # API entry
        response_body += (18).to_bytes(2, "big")  # api_key
        response_body += (0).to_bytes(2, "big")   # min_version
        response_body += (4).to_bytes(2, "big")   # max_version

        # TAG_BUFFER
        response_body += b"\x00"

        # throttle_time_ms
        response_body += (0).to_bytes(4, "big")

        # Final TAG_BUFFER
        response_body += b"\x00"

        # -------------------------
        # Header
        # -------------------------

        response_header = correlation_id

        # message_size excludes its own 4 bytes
        message_size = len(response_header) + len(response_body)

        # Build final response
        response = (
            message_size.to_bytes(4, "big") +
            response_header +
            response_body
        )

        conn.sendall(response)
    
    conn.close()


if __name__ == "__main__":
    main()