import socket
import threading


# =========================================================
# ApiVersions Response
# =========================================================

def build_apiversions_response(correlation_id, api_version):

    if 0 <= api_version <= 4:
        error_code = 0
    else:
        error_code = 35

    response_body = b""

    # error_code
    response_body += error_code.to_bytes(2, "big")

    # COMPACT_ARRAY => 2 elements
    response_body += b"\x03"

    # -----------------------------------------
    # ApiVersions API
    # -----------------------------------------

    response_body += (18).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (4).to_bytes(2, "big")
    response_body += b"\x00"

    # -----------------------------------------
    # DescribeTopicPartitions API
    # -----------------------------------------

    response_body += (75).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += b"\x00"

    # throttle_time_ms
    response_body += (0).to_bytes(4, "big")

    # TAG_BUFFER
    response_body += b"\x00"

    # -----------------------------------------
    # Response Header v0
    # -----------------------------------------

    response_header = correlation_id

    message_size = (
        len(response_header) +
        len(response_body)
    )

    response = (
        message_size.to_bytes(4, "big")
        + response_header
        + response_body
    )

    return response


# =========================================================
# DescribeTopicPartitions Response
# =========================================================

def build_describe_topic_partitions_response(
    correlation_id,
    topic_name
):

    response_body = b""

    # throttle_time_ms
    response_body += (0).to_bytes(4, "big")

    # topics COMPACT_ARRAY
    # 1 topic => stored as 2
    response_body += b"\x02"

    # error_code = UNKNOWN_TOPIC_OR_PARTITION
    response_body += (3).to_bytes(2, "big")

    # -------------------------------------------------
    # COMPACT_STRING topic_name
    # -------------------------------------------------

    topic_length = len(topic_name)

    response_body += bytes([topic_length + 1])

    response_body += topic_name

    # topic_id UUID = 16 zero bytes
    response_body += b"\x00" * 16

    # is_internal = false
    response_body += b"\x00"

    # partitions empty compact array
    response_body += b"\x01"

    # topic_authorized_operations
    response_body += (0).to_bytes(4, "big")

    # TAG_BUFFER
    response_body += b"\x00"

    # next_cursor = null
    response_body += b"\xff"

    # final TAG_BUFFER
    response_body += b"\x00"

    # -------------------------------------------------
    # Response Header v1
    # -------------------------------------------------

    response_header = correlation_id + b"\x00"

    message_size = (
        len(response_header) +
        len(response_body)
    )

    response = (
        message_size.to_bytes(4, "big")
        + response_header
        + response_body
    )

    return response


# =========================================================
# Parse Topic Name Properly
# =========================================================

def parse_topic_name(request):

    # -------------------------------------------------
    # Start after request header
    # -------------------------------------------------

    cursor = 0

    # message_size
    cursor += 4

    # api_key
    cursor += 2

    # api_version
    cursor += 2

    # correlation_id
    cursor += 4

    # -------------------------------------------------
    # client_id (NULLABLE_STRING)
    # -------------------------------------------------

    client_id_length = int.from_bytes(
        request[cursor:cursor + 2],
        "big"
    )

    cursor += 2

    if client_id_length > 0:
        cursor += client_id_length

    # -------------------------------------------------
    # TAG_BUFFER
    # -------------------------------------------------

    cursor += 1

    # -------------------------------------------------
    # topics COMPACT_ARRAY
    # -------------------------------------------------

    cursor += 1

    # -------------------------------------------------
    # topic_name COMPACT_STRING
    # -------------------------------------------------

    topic_length = request[cursor] - 1

    cursor += 1

    topic_name = request[
        cursor:cursor + topic_length
    ]

    return topic_name


# =========================================================
# Handle Client
# =========================================================

def handle_client(conn):

    while True:

        try:

            request = conn.recv(1024)

            if not request:
                break

            # -------------------------------------------------
            # Common Header
            # -------------------------------------------------

            api_key = int.from_bytes(
                request[4:6],
                "big"
            )

            api_version = int.from_bytes(
                request[6:8],
                "big"
            )

            correlation_id = request[8:12]

            # =================================================
            # ApiVersions
            # =================================================

            if api_key == 18:

                response = build_apiversions_response(
                    correlation_id,
                    api_version
                )

                conn.sendall(response)

            # =================================================
            # DescribeTopicPartitions
            # =================================================

            elif api_key == 75:

                topic_name = parse_topic_name(
                    request
                )

                response = (
                    build_describe_topic_partitions_response(
                        correlation_id,
                        topic_name
                    )
                )

                conn.sendall(response)

        except Exception as e:

            print(f"Error: {e}")

            break

    conn.close()


# =========================================================
# Main
# =========================================================

def main():

    print("Kafka broker starting...")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    while True:

        conn, addr = server.accept()

        print(f"Connection from {addr}")

        thread = threading.Thread(
            target=handle_client,
            args=(conn,)
        )

        thread.start()


if __name__ == "__main__":
    main()