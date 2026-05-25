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

    # -------------------------------------------------
    # Parse request step-by-step
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

    print("\nAFTER FIXED HEADER:", cursor)

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

    # -------------------------------------------------
    # Fetch request fields (v16)
    # -------------------------------------------------

    replica_id = int.from_bytes(
        request[cursor:cursor + 4],
        "big",
        signed=True
    )

    print("REPLICA ID:", replica_id)

    cursor += 4

    max_wait_ms = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    print("MAX WAIT MS:", max_wait_ms)

    cursor += 4

    min_bytes = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    print("MIN BYTES:", min_bytes)

    cursor += 4

    max_bytes = int.from_bytes(
        request[cursor:cursor + 4],
        "big",
        signed=True
    )

    print("MAX BYTES:", max_bytes)

    cursor += 4

    isolation_level = request[cursor]

    print("ISOLATION LEVEL:", isolation_level)

    cursor += 1

    session_id = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    print("SESSION ID:", session_id)

    cursor += 4

    session_epoch = int.from_bytes(
        request[cursor:cursor + 4],
        "big"
    )

    print("SESSION EPOCH:", session_epoch)

    cursor += 4

    print("AFTER FETCH HEADER:", cursor)

    # -------------------------------------------------
    # topics compact array
    # -------------------------------------------------

    topics_array_raw = request[cursor]

    print("TOPICS ARRAY RAW:", topics_array_raw)

    topics_count = topics_array_raw - 1

    print("TOPICS COUNT:", topics_count)

    cursor += 1

    # -------------------------------------------------
    # topic_id
    # -------------------------------------------------
    #
    # IMPORTANT:
    # We were off by 1 byte earlier because
    # compact array length consumed only 1 byte.
    #
    # Actual UUID starts immediately here.
    # -------------------------------------------------

    print("UUID START OFFSET:", cursor)

    topic_id = request[
        cursor:
        cursor + 16
    ]

    print("TOPIC UUID FOUND:")

    print(topic_id.hex())

    # -------------------------------------------------
    # Pretty UUID formatting debug
    # -------------------------------------------------

    uuid_hex = topic_id.hex()

    pretty_uuid = (
        f"{uuid_hex[0:8]}-"
        f"{uuid_hex[8:12]}-"
        f"{uuid_hex[12:16]}-"
        f"{uuid_hex[16:20]}-"
        f"{uuid_hex[20:32]}"
    )

    print("FORMATTED UUID:")

    print(pretty_uuid)

    print("========================================\n")

    return topic_id