"""
Binary utility functions for encoding and decoding types in the Kafka protocol.
"""

def encode_varint(value: int) -> bytes:
    """
    Encode an integer as an unsigned varint.
    """
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7f) | 0x80)
        value >>= 7
    out.append(value & 0x7f)
    return bytes(out)


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    """
    Decode an unsigned varint from the byte sequence starting at the given offset.
    Returns a tuple of (decoded_value, next_offset).
    """
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
