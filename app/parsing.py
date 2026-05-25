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


def parse_fetch_topic_id(request):

    print("\n========== FETCH REQUEST DEBUG ==========")

    print("REQUEST LENGTH:", len(request))

    print("REQUEST HEX:")

    print(request.hex())

    print("\n4-BYTE CHUNKS:")

    for i in range(0, len(request), 4):

        chunk = request[i:i+4]

        print(
            f"OFFSET {i:03d} | "
            f"{chunk.hex()}"
        )

    # =================================================
    # PARSE REQUEST HEADER
    # =================================================

    cursor = 0

    # message_size
    cursor += 4

    # api_key
    cursor += 2

    # api_version
    api_version = int.from_bytes(
        request[6:8],
        "big"
    )

    cursor += 2

    print("\nFETCH API VERSION:", api_version)

    # correlation_id
    cursor += 4

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

    tag_buffer = request[cursor]

    print("HEADER TAG BUFFER:", tag_buffer)

    cursor += 1

    # =================================================
    # FETCH REQUEST BODY
    # =================================================

    body_start = cursor

    print("\nBODY START:", body_start)

    # -------------------------------------------------
    # ReplicaId exists only <= v14
    # -------------------------------------------------

    if api_version <= 14:

        replica_id = int.from_bytes(
            request[cursor:cursor+4],
            "big",
            signed=True
        )

        print("REPLICA ID:", replica_id)

        cursor += 4

    # -------------------------------------------------
    # MaxWaitMs
    # -------------------------------------------------

    max_wait_ms = int.from_bytes(
        request[cursor:cursor+4],
        "big"
    )

    print("MAX WAIT MS:", max_wait_ms)

    cursor += 4

    # -------------------------------------------------
    # MinBytes
    # -------------------------------------------------

    min_bytes = int.from_bytes(
        request[cursor:cursor+4],
        "big"
    )

    print("MIN BYTES:", min_bytes)

    cursor += 4

    # -------------------------------------------------
    # MaxBytes
    # -------------------------------------------------

    max_bytes = int.from_bytes(
        request[cursor:cursor+4],
        "big",
        signed=True
    )

    print("MAX BYTES:", max_bytes)

    cursor += 4

    # -------------------------------------------------
    # IsolationLevel
    # -------------------------------------------------

    isolation_level = request[cursor]

    print("ISOLATION LEVEL:", isolation_level)

    cursor += 1

    # -------------------------------------------------
    # SessionId
    # -------------------------------------------------

    session_id = int.from_bytes(
        request[cursor:cursor+4],
        "big"
    )

    print("SESSION ID:", session_id)

    cursor += 4

    # -------------------------------------------------
    # SessionEpoch
    # -------------------------------------------------

    session_epoch = int.from_bytes(
        request[cursor:cursor+4],
        "big"
    )

    print("SESSION EPOCH:", session_epoch)

    cursor += 4

    print("AFTER FETCH HEADER:", cursor)

    # =================================================
    # TOPICS ARRAY
    # =================================================

    topics_array_raw = request[cursor]

    print("TOPICS ARRAY RAW:", topics_array_raw)

    topics_count = topics_array_raw - 1

    print("TOPICS COUNT:", topics_count)

    cursor += 1

    # -------------------------------------------------
    # Validate topic count
    # -------------------------------------------------

    if topics_count <= 0:

        print("NO TOPICS FOUND")

        return b"\x00" * 16

    # =================================================
    # TOPIC UUID
    # =================================================

    topic_id = request[
        cursor:
        cursor + 16
    ]

    print("\nTOPIC UUID FOUND:")

    print(topic_id.hex())

    print("UUID OFFSET:", cursor)

    cursor += 16

    # =================================================
    # PARTITIONS ARRAY
    # =================================================

    partitions_raw = request[cursor]

    print("\nPARTITIONS ARRAY RAW:", partitions_raw)

    partitions_count = partitions_raw - 1

    print("PARTITIONS COUNT:", partitions_count)

    cursor += 1

    print("\n========== FINAL FETCH PARSE ==========")

    print("FINAL CURSOR:", cursor)

    print("REMAINING BYTES:")

    print(request[cursor:].hex())

    print("=======================================\n")

    return topic_id