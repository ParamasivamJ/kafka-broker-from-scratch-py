from binary_utils import encode_varint

def build_fetch_response_with_records(
    correlation_id,
    topic_id,
    partition_index,
    record_bytes
):
    """
    Stage EG2: Topic has a message.
    Returns error_code=0 with the record bytes.
    """
    print("\n========== RECORDS FETCH RESPONSE ==========")
    print("TOPIC ID:", topic_id.hex())
    print("PARTITION INDEX:", partition_index)
    print("RECORD BYTES SIZE:", len(record_bytes))

    response_body = b""

    # throttle_time_ms = 0
    response_body += (0).to_bytes(4, "big")

    # error_code = 0 (NO_ERROR)
    response_body += (0).to_bytes(2, "big")

    # session_id = 0
    response_body += (0).to_bytes(4, "big")

    # responses compact array: 1 element => length = 2
    response_body += b"\x02"

    # topic_id (16 bytes UUID)
    response_body += topic_id

    # partitions compact array: 1 element => length = 2
    response_body += b"\x02"

    # partition_index
    response_body += partition_index.to_bytes(4, "big")

    # error_code = 0 (NO_ERROR)
    response_body += (0).to_bytes(2, "big")

    # high_watermark = 1
    response_body += (1).to_bytes(8, "big")

    # last_stable_offset = 0
    response_body += (0).to_bytes(8, "big")

    # log_start_offset = 0
    response_body += (0).to_bytes(8, "big")

    # aborted_transactions compact array: 0 elements => length = 1
    response_body += b"\x01"

    # preferred_read_replica = -1
    response_body += (-1).to_bytes(4, "big", signed=True)

    # records: COMPACT_NULLABLE_BYTES
    # length + 1 as UNSIGNED_VARINT, then data
    records_length_varint = encode_varint(len(record_bytes) + 1)
    response_body += records_length_varint
    response_body += record_bytes

    # partition TAG_BUFFER
    response_body += b"\x00"

    # topic TAG_BUFFER
    response_body += b"\x00"

    # final TAG_BUFFER
    response_body += b"\x00"

    response_header = correlation_id + b"\x00"

    message_size = len(response_header) + len(response_body)

    response = (
        message_size.to_bytes(4, "big") +
        response_header +
        response_body
    )

    print("RECORDS RESPONSE HEX:")
    print(response.hex()[:200] + "...")
    print("============================================\n")

    return response


def build_fetch_response_empty_records(
    correlation_id,
    topic_id,
    partition_index=0
):
    """
    Stage CM4: Topic exists but has no messages.
    Returns error_code=0 with 0 records.
    """
    print("\n========== EMPTY RECORDS FETCH RESPONSE ==========")
    print("TOPIC ID:", topic_id.hex())
    print("PARTITION INDEX:", partition_index)

    response_body = b""

    # throttle_time_ms = 0
    response_body += (0).to_bytes(4, "big")

    # error_code = 0 (NO_ERROR)
    response_body += (0).to_bytes(2, "big")

    # session_id = 0
    response_body += (0).to_bytes(4, "big")

    # responses compact array: 1 element => length = 2
    response_body += b"\x02"

    # topic_id (16 bytes UUID)
    response_body += topic_id

    # partitions compact array: 1 element => length = 2
    response_body += b"\x02"

    # partition_index
    response_body += partition_index.to_bytes(4, "big")

    # error_code = 0 (NO_ERROR)
    response_body += (0).to_bytes(2, "big")

    # high_watermark
    response_body += (0).to_bytes(8, "big")

    # last_stable_offset
    response_body += (0).to_bytes(8, "big")

    # log_start_offset
    response_body += (0).to_bytes(8, "big")

    # aborted_transactions compact array: 0 elements => length = 1
    response_body += b"\x01"

    # preferred_read_replica = -1
    response_body += (-1).to_bytes(4, "big", signed=True)

    # records compact array: 0 elements => length = 1
    response_body += b"\x01"

    # partition TAG_BUFFER
    response_body += b"\x00"

    # topic TAG_BUFFER
    response_body += b"\x00"

    # final TAG_BUFFER
    response_body += b"\x00"

    response_header = correlation_id + b"\x00"

    message_size = len(response_header) + len(response_body)

    response = (
        message_size.to_bytes(4, "big") +
        response_header +
        response_body
    )

    print("EMPTY RECORDS RESPONSE HEX:")
    print(response.hex())
    print("==================================================\n")

    return response


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

def build_fetch_response_empty_topics(
    correlation_id
):

    print("\n========== EMPTY FETCH RESPONSE ==========")

    response_body = b""

    # -------------------------------------------------
    # throttle_time_ms
    # -------------------------------------------------

    response_body += (0).to_bytes(
        4,
        "big"
    )

    print("THROTTLE TIME: 0")

    # -------------------------------------------------
    # error_code
    # -------------------------------------------------

    response_body += (0).to_bytes(
        2,
        "big"
    )

    print("ERROR CODE: 0")

    # -------------------------------------------------
    # session_id
    # -------------------------------------------------

    response_body += (0).to_bytes(
        4,
        "big"
    )

    print("SESSION ID: 0")

    # -------------------------------------------------
    # responses array
    #
    # EMPTY ARRAY
    #
    # compact array:
    # 0 elements => 1
    # -------------------------------------------------

    response_body += b"\x01"

    print("RESPONSES ARRAY: EMPTY")

    # -------------------------------------------------
    # TAG_BUFFER
    # -------------------------------------------------

    response_body += b"\x00"

    print("FINAL TAG BUFFER")

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

    print("FETCH EMPTY RESPONSE HEX:")

    print(response.hex())

    print("==========================================\n")

    return response