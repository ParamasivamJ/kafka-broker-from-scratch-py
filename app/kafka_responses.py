from .kafka_metadata import load_metadata, extract_partitions, serialize_partition


def build_apiversions_response(correlation_id, api_version):
    if 0 <= api_version <= 4:
        error_code = 0
    else:
        error_code = 35

    response_body = b""
    response_body += error_code.to_bytes(2, "big")
    response_body += b"\x03"

    response_body += (18).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (4).to_bytes(2, "big")
    response_body += b"\x00"

    response_body += (75).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += (0).to_bytes(2, "big")
    response_body += b"\x00"

    response_body += (0).to_bytes(4, "big")
    response_body += b"\x00"

    response_header = correlation_id
    message_size = len(response_header) + len(response_body)

    return (
        message_size.to_bytes(4, "big")
        + response_header
        + response_body
    )


def build_describe_topic_partitions_response(correlation_id, topics):
    metadata = load_metadata()
    response_body = b""
    response_body += (0).to_bytes(4, "big")
    response_body += bytes([len(topics) + 1])

    for topic_name in topics:
        print("\n========== TOPIC RESPONSE ==========")
        print("TOPIC:", topic_name)

        topic_index = metadata.find(topic_name)
        if topic_index != -1:
            error_code = 0
            print("TOPIC FOUND")
            uuid_start = topic_index + len(topic_name)
            topic_uuid = metadata[uuid_start:uuid_start + 16]
            partitions = extract_partitions(metadata, topic_uuid)
        else:
            print("TOPIC NOT FOUND")
            error_code = 3
            topic_uuid = b"\x00" * 16
            partitions = []

        print("ERROR CODE:", error_code)
        print("UUID:", topic_uuid.hex())
        print("PARTITIONS:", partitions)

        response_body += error_code.to_bytes(2, "big")
        response_body += bytes([len(topic_name) + 1])
        response_body += topic_name
        response_body += topic_uuid
        response_body += b"\x00"
        response_body += bytes([len(partitions) + 1])

        for partition_index in partitions:
            print(f"SERIALIZING PARTITION {partition_index}")
            response_body += serialize_partition(partition_index)

        response_body += (0).to_bytes(4, "big")
        response_body += b"\x00"

    response_body += b"\xff"
    response_body += b"\x00"

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
