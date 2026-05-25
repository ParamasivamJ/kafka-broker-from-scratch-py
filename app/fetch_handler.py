def build_fetch_unknown_topic_response(
    correlation_id,
    topic_id
):

    print("\n========== UNKNOWN TOPIC FETCH ==========")

    print("TOPIC ID:", topic_id.hex())

    response_body = b""

    # -------------------------------------------------
    # throttle_time_ms
    # -------------------------------------------------

    response_body += (0).to_bytes(
        4,
        "big"
    )

    # -------------------------------------------------
    # error_code
    # -------------------------------------------------

    response_body += (0).to_bytes(
        2,
        "big"
    )

    # -------------------------------------------------
    # session_id
    # -------------------------------------------------

    response_body += (0).to_bytes(
        4,
        "big"
    )

    # -------------------------------------------------
    # responses array
    #
    # 1 element => 1 + 1 = 2
    # -------------------------------------------------

    response_body += b"\x02"

    # -------------------------------------------------
    # topic_id
    # -------------------------------------------------

    response_body += topic_id

    # -------------------------------------------------
    # partitions array
    #
    # 1 partition
    # -------------------------------------------------

    response_body += b"\x02"

    # -------------------------------------------------
    # partition_index
    # -------------------------------------------------

    response_body += (0).to_bytes(
        4,
        "big"
    )

    # -------------------------------------------------
    # error_code = 100
    # UNKNOWN_TOPIC_ID
    # -------------------------------------------------

    response_body += (100).to_bytes(
        2,
        "big"
    )

    print("PARTITION ERROR: 100")

    # -------------------------------------------------
    # minimal placeholder fields
    # -------------------------------------------------

    response_body += (0).to_bytes(8, "big")
    response_body += (0).to_bytes(8, "big")
    response_body += (0).to_bytes(8, "big")

    # aborted transactions
    response_body += b"\x01"

    # preferred read replica
    response_body += (-1).to_bytes(
        4,
        "big",
        signed=True
    )

    # records
    response_body += b"\x01"

    # partition TAG_BUFFER
    response_body += b"\x00"

    # topic TAG_BUFFER
    response_body += b"\x00"

    # final TAG_BUFFER
    response_body += b"\x00"

    response_header = (
        correlation_id +
        b"\x00"
    )

    message_size = (
        len(response_header) +
        len(response_body)
    )

    response = (
        message_size.to_bytes(4, "big") +
        response_header +
        response_body
    )

    print("FETCH UNKNOWN TOPIC RESPONSE:")

    print(response.hex())

    print("========================================\n")

    return response