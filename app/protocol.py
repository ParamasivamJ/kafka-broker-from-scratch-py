from metadata_utils import load_metadata, find_topic_metadata,extract_partitions


def build_apiversions_response(correlation_id, api_version):
    error_code = 0 if 0 <= api_version <= 4 else 35

    response_body = b""

    # error_code
    response_body += error_code.to_bytes(2, "big")

    # 4 APIs → compact array = 4 + 1 = 5
    response_body += b"\x05"

    # API 0 (Produce)
    response_body += (0).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (11).to_bytes(2, "big")
    response_body += b"\x00"

    # API 18 (ApiVersions)
    response_body += (18).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (4).to_bytes(2, "big")
    response_body += b"\x00"

    # API 1 (Fetch)
    response_body += (1).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (16).to_bytes(2, "big")
    response_body += b"\x00"

    # API 75 (DescribeTopicPartitions)
    response_body += (75).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += b"\x00"

    # throttle_time_ms
    response_body += (0).to_bytes(4, "big")

    # final TAG_BUFFER
    response_body += b"\x00"

    response_header = correlation_id
    message_size = len(response_header) + len(response_body)

    return (
        message_size.to_bytes(4, "big")
        + response_header
        + response_body
    )


def serialize_partition(partition_index):
    data = b""

    data += (0).to_bytes(2, "big")           # error_code
    data += partition_index.to_bytes(4, "big")  # partition_index
    data += (1).to_bytes(4, "big")           # leader_id
    data += (0).to_bytes(4, "big")           # leader_epoch

    data += b"\x02"                          # replica_nodes length = 1
    data += (1).to_bytes(4, "big")           # replica id 1

    data += b"\x02"                          # isr_nodes length = 1
    data += (1).to_bytes(4, "big")           # isr id 1

    data += b"\x01"                          # eligible_leader_replicas empty
    data += b"\x01"                          # last_known_elr empty
    data += b"\x01"                          # offline_replicas empty
    data += b"\x00"                          # TAG_BUFFER

    return data


def build_describe_topic_partitions_response(correlation_id, topics):
    metadata = load_metadata()

    response_body = b""

    # throttle_time_ms
    response_body += (0).to_bytes(4, "big")

    # TOPICS ARRAY
    response_body += bytes([len(topics) + 1])

    for topic_name in topics:
        print("\n========== TOPIC RESPONSE ==========")
        print("TOPIC:", topic_name)

        topic_metadata = find_topic_metadata(
            metadata,
            topic_name
        )

        if topic_metadata:

            error_code = 0

            topic_uuid = topic_metadata["uuid"]

            partitions = extract_partitions(
                metadata,
                topic_uuid
            )

        else:

            error_code = 3

            topic_uuid = b"\x00" * 16

            partitions = []

        # Topic error_code
        response_body += error_code.to_bytes(2, "big")

        # Topic name
        response_body += bytes([len(topic_name) + 1])
        response_body += topic_name

        # Topic UUID
        response_body += topic_uuid

        # is_internal
        response_body += b"\x00"

        # partitions array
        response_body += bytes([len(partitions) + 1])

        for partition_index in partitions:
            print(f"SERIALIZING PARTITION {partition_index}")
            response_body += serialize_partition(partition_index)

        # topic_authorized_operations
        response_body += (0).to_bytes(4, "big")

        # TAG_BUFFER
        response_body += b"\x00"

    # next_cursor => null
    response_body += b"\xff"

    # final TAG_BUFFER
    response_body += b"\x00"

    # Response Header v1
    response_header = correlation_id + b"\x00"

    message_size = len(response_header) + len(response_body)

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


def build_produce_response(correlation_id, topics_data):
    def encode_varint(value):
        out = bytearray()
        while value >= 0x80:
            out.append((value & 0x7f) | 0x80)
            value >>= 7
        out.append(value & 0x7f)
        return bytes(out)

    response_body = b""

    # Responses compact array: count + 1
    response_body += encode_varint(len(topics_data) + 1)

    for topic in topics_data:
        topic_name = topic["name"]
        partitions = topic["partitions"]

        # Name (Compact String)
        topic_name_bytes = topic_name.encode("utf-8")
        response_body += encode_varint(len(topic_name_bytes) + 1)
        response_body += topic_name_bytes

        # PartitionResponses compact array: count + 1
        response_body += encode_varint(len(partitions) + 1)

        for part in partitions:
            partition_index = part["index"]
            error_code = part["error_code"]

            # Index: int32 (4 bytes)
            response_body += partition_index.to_bytes(4, "big")

            # ErrorCode: int16 (2 bytes)
            response_body += error_code.to_bytes(2, "big")

            if error_code == 0:
                base_offset = 0
                log_start_offset = 0
            else:
                base_offset = -1
                log_start_offset = -1

            # BaseOffset: int64 (8 bytes)
            response_body += base_offset.to_bytes(8, "big", signed=True)

            # LogAppendTimeMs: int64 (8 bytes) -> -1
            response_body += (-1).to_bytes(8, "big", signed=True)

            # LogStartOffset: int64 (8 bytes)
            response_body += log_start_offset.to_bytes(8, "big", signed=True)

            # RecordErrors compact array: 0 elements => length = 1
            response_body += b"\x01"

            # ErrorMessage: Compact Nullable String -> null (b"\x00")
            response_body += b"\x00"

            # Partition tag buffer: \x00
            response_body += b"\x00"

        # Topic tag buffer: \x00
        response_body += b"\x00"

    # ThrottleTimeMs: int32 (4 bytes) -> 0
    response_body += (0).to_bytes(4, "big")

    # Response tag buffer: \x00
    response_body += b"\x00"

    # Response Header Version 1
    response_header = correlation_id + b"\x00"

    message_size = len(response_header) + len(response_body)

    response = (
        message_size.to_bytes(4, "big") +
        response_header +
        response_body
    )

    print("\n========== PRODUCE RESPONSE ==========")
    print("TOPICS DATA:", topics_data)
    print("RESPONSE HEX:", response.hex())
    print("======================================\n")

    return response