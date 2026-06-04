"""
KRaft cluster metadata utilities.

Reads the binary __cluster_metadata log to discover topic UUIDs,
topic names, and partition assignments.
"""

from config import METADATA_PATH


def load_metadata() -> bytes:
    """Load the raw KRaft cluster-metadata log into memory."""
    print("\n========== LOAD METADATA ==========")
    print("METADATA PATH:", METADATA_PATH)

    with open(METADATA_PATH, "rb") as f:
        data = f.read()

    print("METADATA SIZE:", len(data))
    print("===================================\n")
    return data


def extract_partitions(metadata: bytes, topic_uuid: bytes) -> list[int]:
    """
    Scan the metadata log for all partition records associated with *topic_uuid*.

    Partition records in KRaft store ``[partition_id (4B)] [topic_uuid (16B)]``.
    We search for every occurrence of the UUID and inspect the 4 bytes before it.
    """
    partitions = []
    search_start = 0

    print("\n========== PARTITION DEBUG ==========")
    print("TOPIC UUID:", topic_uuid.hex())

    while True:
        uuid_index = metadata.find(topic_uuid, search_start)

        if uuid_index == -1:
            break

        print("UUID INDEX:", uuid_index)

        if uuid_index >= 4:
            partition_bytes = metadata[uuid_index - 4:uuid_index]
            partition_id = int.from_bytes(partition_bytes, "big")

            if 0 <= partition_id <= 100:
                print(f"FOUND PARTITION: {partition_id}")
                partitions.append(partition_id)

        search_start = uuid_index + 16

    partitions = sorted(list(set(partitions)))

    print("FINAL PARTITIONS:", partitions)
    print("=====================================\n")

    if not partitions:
        return [0]

    return partitions


def topic_uuid_exists(metadata: bytes, topic_uuid: bytes) -> bool:
    """Return True if *topic_uuid* appears anywhere in the metadata log."""
    found = metadata.find(topic_uuid) != -1
    print(
        "TOPIC UUID EXISTS:",
        found,
        "| UUID:", topic_uuid.hex()
    )
    return found


def find_topic_name_by_uuid(metadata: bytes, topic_uuid: bytes) -> str | None:
    """
    Reverse-search the metadata log to find the topic name for a given UUID.

    In KRaft, topic creation records store:
        ``[compact_string_length] [topic_name] [topic_uuid (16B)]``

    We locate the UUID, then walk backwards to find the compact-string length
    byte that matches.
    """
    uuid_index = metadata.find(topic_uuid)
    if uuid_index == -1:
        return None

    for length in range(1, 100):
        if uuid_index - length - 1 >= 0:
            len_byte = metadata[uuid_index - length - 1]
            if len_byte == length + 1:
                topic_name_bytes = metadata[uuid_index - length:uuid_index]
                if all(32 <= b <= 126 for b in topic_name_bytes):
                    try:
                        name = topic_name_bytes.decode("utf-8")
                        print(f"FOUND TOPIC NAME FOR UUID {topic_uuid.hex()}: {name}")
                        return name
                    except Exception:
                        pass
    return None


def find_topic_metadata(metadata: bytes, topic_name: bytes) -> dict | None:
    """
    Search the metadata log for an encoded topic name and return its UUID.

    Returns ``{"uuid": <16-byte UUID>}`` on success, or ``None`` if not found.
    """
    print("\n========== FIND TOPIC ==========")
    print("TOPIC:", topic_name)

    # Compact string encoding: [length + 1] [name bytes]
    encoded_topic = bytes([len(topic_name) + 1]) + topic_name

    print("ENCODED TOPIC:")
    print(encoded_topic.hex())

    topic_index = metadata.find(encoded_topic)

    print("TOPIC INDEX:", topic_index)

    if topic_index == -1:
        print("TOPIC NOT FOUND")
        print("================================\n")
        return None

    # The 16-byte UUID follows immediately after the encoded topic name
    uuid_start = topic_index + len(encoded_topic)
    topic_uuid = metadata[uuid_start:uuid_start + 16]

    print("UUID:", topic_uuid.hex())
    print("================================\n")

    return {"uuid": topic_uuid}