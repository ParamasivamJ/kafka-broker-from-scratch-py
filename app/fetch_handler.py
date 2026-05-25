def build_fetch_response(correlation_id):

    print("\n========== FETCH RESPONSE ==========")

    response_body = b""

    # =========================================
    # throttle_time_ms
    # =========================================

    response_body += (0).to_bytes(
        4,
        "big"
    )

    print("THROTTLE TIME: 0")

    # =========================================
    # error_code
    # =========================================

    response_body += (0).to_bytes(
        2,
        "big"
    )

    print("ERROR CODE: 0")

    # =========================================
    # session_id
    # =========================================

    response_body += (0).to_bytes(
        4,
        "big"
    )

    print("SESSION ID: 0")

    # =========================================
    # responses array
    # Compact array:
    # 0 elements => 0 + 1 => 1
    # =========================================

    response_body += b"\x01"

    print("RESPONSES ARRAY: EMPTY")

    # =========================================
    # TAG_BUFFER
    # =========================================

    response_body += b"\x00"

    # =========================================
    # Response Header v1
    # =========================================

    response_header = (
        correlation_id
        + b"\x00"
    )

    message_size = (
        len(response_header)
        + len(response_body)
    )

    response = (
        message_size.to_bytes(4, "big")
        + response_header
        + response_body
    )

    print("\nFETCH RESPONSE HEX:")

    print(response.hex())

    print("====================================\n")

    return response