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
    # Skip Fetch fields before topics
    # -------------------------------------------------

    cursor += 4   # replica_id
    cursor += 4   # max_wait_ms
    cursor += 4   # min_bytes
    cursor += 4   # max_bytes
    cursor += 1   # isolation_level
    cursor += 4   # session_id
    cursor += 4   # session_epoch

    print("AFTER FETCH HEADER:", cursor)

    # -------------------------------------------------
    # topics compact array
    # -------------------------------------------------

    topics_array_length = request[cursor]

    print("TOPICS ARRAY RAW:", topics_array_length)

    topics_count = topics_array_length - 1

    print("TOPICS COUNT:", topics_count)

    cursor += 1

    # -------------------------------------------------
    # topic_id
    # -------------------------------------------------

    topic_id = request[
        cursor:
        cursor + 16
    ]

    print("TOPIC UUID FOUND:")

    print(topic_id.hex())

    print("UUID OFFSET:", cursor)

    print("========================================\n")

    return topic_id