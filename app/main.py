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

def extract_partitions(metadata, topic_uuid):

    partitions = []

    search_start = 0

    while True:

        # -----------------------------------------
        # Find next UUID occurrence
        # -----------------------------------------

        uuid_index = metadata.find(
            topic_uuid,
            search_start
        )

        if uuid_index == -1:
            break

        # -----------------------------------------
        # Partition ID is 4 bytes BEFORE UUID
        # -----------------------------------------

        if uuid_index >= 4:

            partition_bytes = metadata[
                uuid_index - 4:
                uuid_index
            ]

            partition_id = int.from_bytes(
                partition_bytes,
                "big"
            )

            # Avoid garbage values
            if 0 <= partition_id <= 100:

                print(
                    f"FOUND PARTITION:"
                    f" {partition_id}"
                )

                partitions.append(
                    partition_id
                )

        # Continue searching
        search_start = uuid_index + 16

    # Remove duplicates
    partitions = sorted(
        list(set(partitions))
    )

    print("FINAL PARTITIONS:", partitions)

    # Fallback
    if not partitions:
        return [0]

    return partitions

def serialize_partition(partition_index):

    data = b""

    # error_code
    data += (0).to_bytes(2, "big")

    # partition_index
    data += partition_index.to_bytes(4, "big")

    # leader_id
    data += (1).to_bytes(4, "big")

    # leader_epoch
    data += (0).to_bytes(4, "big")

    # replica_nodes
    data += b"\x02"
    data += (1).to_bytes(4, "big")

    # isr_nodes
    data += b"\x02"
    data += (1).to_bytes(4, "big")

    # eligible_leader_replicas
    data += b"\x01"

    # last_known_elr
    data += b"\x01"

    # offline_replicas
    data += b"\x01"

    # TAG_BUFFER
    data += b"\x00"

    return data


def build_describe_topic_partitions_response(
    correlation_id,
    topics
):

    # -------------------------------------------------
    # Load metadata log
    # -------------------------------------------------

    metadata_path = (
        "/tmp/kraft-combined-logs/"
        "__cluster_metadata-0/"
        "00000000000000000000.log"
    )

    with open(metadata_path, "rb") as f:
        metadata = f.read()

    # -------------------------------------------------
    # Find topic UUID
    # -------------------------------------------------

    topic_index = metadata.find(topic_name)

    if topic_index != -1:

        error_code = 0

        print("TOPIC FOUND:", topic_name)
        print("TOPIC INDEX:", topic_index)

        start = max(0, topic_index - 64)
        end = topic_index + 64

        print(metadata[start:end].hex())

        # -------------------------------------------------
        # UUID starts immediately after topic name
        # -------------------------------------------------

        uuid_start = topic_index + len(topic_name)

        topic_uuid = metadata[
            uuid_start:
            uuid_start + 16
        ]

    else:

        error_code = 3

        topic_uuid = b"\x00" * 16

    # -------------------------------------------------
    # Build response body
    # -------------------------------------------------

    response_body = b""

    # throttle_time_ms
    response_body += (0).to_bytes(4, "big")

        # =================================================
    # TOPICS ARRAY
    # =================================================

    response_body += bytes([
        len(topics) + 1
    ])

    for topic_name in topics:

        print("\n========== TOPIC RESPONSE ==========")

        print("TOPIC:", topic_name)

        # -------------------------------------------------
        # Find topic UUID
        # -------------------------------------------------

        topic_index = metadata.find(topic_name)

        if topic_index != -1:

            error_code = 0

            print("TOPIC FOUND")

            uuid_start = (
                topic_index +
                len(topic_name)
            )

            topic_uuid = metadata[
                uuid_start:
                uuid_start + 16
            ]

            partitions = extract_partitions(
                metadata,
                topic_uuid
            )

        else:

            print("TOPIC NOT FOUND")

            error_code = 3

            topic_uuid = b"\x00" * 16

            partitions = []

        print("ERROR CODE:", error_code)

        print("UUID:", topic_uuid.hex())

        print("PARTITIONS:", partitions)

        # -------------------------------------------------
        # Topic Error Code
        # -------------------------------------------------

        response_body += error_code.to_bytes(
            2,
            "big"
        )

        # -------------------------------------------------
        # Topic Name
        # -------------------------------------------------

        response_body += bytes([
            len(topic_name) + 1
        ])

        response_body += topic_name

        # -------------------------------------------------
        # Topic UUID
        # -------------------------------------------------

        response_body += topic_uuid

        # -------------------------------------------------
        # is_internal
        # -------------------------------------------------

        response_body += b"\x00"

        # -------------------------------------------------
        # partitions array
        # -------------------------------------------------

        response_body += bytes([
            len(partitions) + 1
        ])

        for partition_index in partitions:

            print(
                f"SERIALIZING PARTITION "
                f"{partition_index}"
            )

            response_body += serialize_partition(
                partition_index
            )

        # -------------------------------------------------
        # topic_authorized_operations
        # -------------------------------------------------

        response_body += (0).to_bytes(
            4,
            "big"
        )

        # -------------------------------------------------
        # TAG_BUFFER
        # -------------------------------------------------

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
    print("\n========== FINAL RESPONSE ==========")

    print("HEADER SIZE:", len(response_header))

    print("BODY SIZE:", len(response_body))

    print("MESSAGE SIZE:", message_size)

    print("RESPONSE HEX:")

    print(response.hex())

    print("====================================\n")

    return response


# =========================================================
# Parse Topic Name Properly
# =========================================================

# =========================================================
# Parse Multiple Topics
# =========================================================

def parse_topics(request):

    print("\n========== PARSE TOPICS ==========")

    cursor = 0

    # -------------------------------------------------
    # Request Header
    # -------------------------------------------------

    cursor += 4  # message_size
    cursor += 2  # api_key
    cursor += 2  # api_version
    cursor += 4  # correlation_id

    print("AFTER FIXED HEADER:", cursor)

    # -------------------------------------------------
    # client_id
    # -------------------------------------------------

    client_id_length = int.from_bytes(
        request[cursor:cursor + 2],
        "big"
    )

    print("CLIENT ID LENGTH:", client_id_length)

    cursor += 2

    if client_id_length > 0:

        client_id = request[
            cursor:
            cursor + client_id_length
        ]

        print("CLIENT ID:", client_id)

        cursor += client_id_length

    print("AFTER CLIENT ID:", cursor)

    # -------------------------------------------------
    # Request Header TAG_BUFFER
    # -------------------------------------------------

    tag_buffer = request[cursor]

    print("HEADER TAG BUFFER:", tag_buffer)

    cursor += 1

    # -------------------------------------------------
    # Topics COMPACT_ARRAY
    # -------------------------------------------------

    topics_count = request[cursor] - 1

    print("TOPICS COUNT:", topics_count)

    cursor += 1

    topics = []

    # -------------------------------------------------
    # Parse each topic
    # -------------------------------------------------

    for i in range(topics_count):

        print(f"\n--- TOPIC {i} ---")

        topic_length = request[cursor] - 1

        print("TOPIC LENGTH:", topic_length)

        cursor += 1

        topic_name = request[
            cursor:
            cursor + topic_length
        ]

        print("TOPIC NAME:", topic_name)

        cursor += topic_length

        # topic TAG_BUFFER
        topic_tag = request[cursor]

        print("TOPIC TAG BUFFER:", topic_tag)

        cursor += 1

        topics.append(topic_name)

    print("\nTOPICS BEFORE SORT:")
    print(topics)

    topics.sort()

    print("\nTOPICS AFTER SORT:")
    print(topics)

    print("\n==================================\n")

    return topics


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

                topics = parse_topics(
                    request
                )

                response = (
                    build_describe_topic_partitions_response(
                        correlation_id,
                        topics
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