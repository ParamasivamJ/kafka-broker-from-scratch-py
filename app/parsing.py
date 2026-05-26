def parse_topics(request):
    print("\n========== PARSE TOPICS ==========")

    cursor = 0

    # Request header
    cursor += 4  # message_size
    cursor += 2  # api_key
    cursor += 2  # api_version
    cursor += 4  # correlation_id

    print("AFTER FIXED HEADER:", cursor)

    # client_id
    client_id_length = int.from_bytes(
        request[cursor:cursor + 2],
        "big",
    )

    print("CLIENT ID LENGTH:", client_id_length)

    cursor += 2

    if client_id_length > 0:
        client_id = request[cursor:cursor + client_id_length]
        print("CLIENT ID:", client_id)
        cursor += client_id_length

    print("AFTER CLIENT ID:", cursor)

    # Request header TAG_BUFFER
    tag_buffer = request[cursor]
    print("HEADER TAG BUFFER:", tag_buffer)
    cursor += 1

    # Topics COMPACT_ARRAY
    topics_count = request[cursor] - 1
    print("TOPICS COUNT:", topics_count)
    cursor += 1

    topics = []

    for i in range(topics_count):
        print(f"\n--- TOPIC {i} ---")

        topic_length = request[cursor] - 1
        print("TOPIC LENGTH:", topic_length)
        cursor += 1

        topic_name = request[cursor:cursor + topic_length]
        print("TOPIC NAME:", topic_name)
        cursor += topic_length

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


def parse_fetch_request(request):

    print("\n========== FETCH REQUEST DEBUG ==========")

    print("REQUEST LENGTH:", len(request))

    print("REQUEST HEX:")

    print(request.hex())

    cursor = 0

    # -------------------------------------------------
    # Fixed Header
    # -------------------------------------------------

    cursor += 4
    cursor += 2

    api_version = int.from_bytes(
        request[cursor:cursor + 2],
        "big"
    )

    cursor += 2

    cursor += 4

    print("FETCH API VERSION:", api_version)

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

    client_id = request[
        cursor:
        cursor + client_id_length
    ]

    print("CLIENT ID:", client_id)

    cursor += client_id_length

    print("AFTER CLIENT ID:", cursor)

    # -------------------------------------------------
    # header TAG_BUFFER
    # -------------------------------------------------

    header_tag = request[cursor]

    print("HEADER TAG BUFFER:", header_tag)

    cursor += 1

    # -------------------------------------------------
    # Fetch Request Fields
    # -------------------------------------------------

    print("\nBODY START:", cursor)

    max_wait_ms = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    cursor += 4

    print("MAX WAIT MS:", max_wait_ms)

    min_bytes = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    cursor += 4

    print("MIN BYTES:", min_bytes)

    max_bytes = int.from_bytes(
        request[cursor:cursor + 4],
        "big",
        signed=True
    )

    cursor += 4

    print("MAX BYTES:", max_bytes)

    isolation_level = request[cursor]

    cursor += 1

    print("ISOLATION LEVEL:", isolation_level)

    session_id = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    cursor += 4

    print("SESSION ID:", session_id)

    session_epoch = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    cursor += 4

    print("SESSION EPOCH:", session_epoch)

    print("AFTER FETCH HEADER:", cursor)

    # -------------------------------------------------
    # Topics COMPACT_ARRAY
    # -------------------------------------------------

    def read_varint(data, offset):
        val = 0
        shift = 0
        while True:
            b = data[offset]
            offset += 1
            val |= (b & 0x7f) << shift
            if not (b & 0x80):
                break
            shift += 7
        return val, offset

    topics_raw, cursor = read_varint(request, cursor)
    topics_count = topics_raw - 1

    print("TOPICS ARRAY RAW:", topics_raw)
    print("TOPICS COUNT:", topics_count)

    if topics_count == 0:
        print("NO TOPICS FOUND")
        return {
            "topics_count": 0,
            "topic_id": None,
            "partition_index": 0
        }

    # -------------------------------------------------
    # Topic UUID
    # -------------------------------------------------

    topic_id = request[cursor:cursor + 16]
    cursor += 16

    print("TOPIC UUID:", topic_id.hex())

    # -------------------------------------------------
    # Partitions COMPACT_ARRAY
    # -------------------------------------------------
    partitions_raw, cursor = read_varint(request, cursor)
    partitions_count = partitions_raw - 1
    print("PARTITIONS COUNT:", partitions_count)

    partitions = []
    for _ in range(partitions_count):
        partition_index = int.from_bytes(request[cursor:cursor+4], "big")
        cursor += 4

        current_leader_epoch = int.from_bytes(request[cursor:cursor+4], "big")
        cursor += 4

        fetch_offset = int.from_bytes(request[cursor:cursor+8], "big")
        cursor += 8

        last_fetched_epoch = int.from_bytes(request[cursor:cursor+4], "big")
        cursor += 4

        log_start_offset = int.from_bytes(request[cursor:cursor+8], "big")
        cursor += 8

        partition_max_bytes = int.from_bytes(request[cursor:cursor+4], "big")
        cursor += 4

        # partition TAG_BUFFER
        _, cursor = read_varint(request, cursor)

        partitions.append({
            "partition_index": partition_index,
            "current_leader_epoch": current_leader_epoch,
            "fetch_offset": fetch_offset,
            "last_fetched_epoch": last_fetched_epoch,
            "log_start_offset": log_start_offset,
            "partition_max_bytes": partition_max_bytes
        })

    # topic TAG_BUFFER
    _, cursor = read_varint(request, cursor)

    target_partition_index = partitions[0]["partition_index"] if partitions else 0
    print("PARTITION INDEX:", target_partition_index)

    return {
        "topics_count": topics_count,
        "topic_id": topic_id,
        "partition_index": target_partition_index
    }