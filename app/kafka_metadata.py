METADATA_PATH = (
    "/tmp/kraft-combined-logs/"
    "__cluster_metadata-0/"
    "00000000000000000000.log"
)


def load_metadata():
    with open(METADATA_PATH, "rb") as f:
        return f.read()


def extract_partitions(metadata, topic_uuid):
    partitions = []
    search_start = 0

    while True:
        uuid_index = metadata.find(topic_uuid, search_start)
        if uuid_index == -1:
            break

        if uuid_index >= 4:
            partition_bytes = metadata[uuid_index - 4:uuid_index]
            partition_id = int.from_bytes(partition_bytes, "big")

            if 0 <= partition_id <= 100:
                print(f"FOUND PARTITION: {partition_id}")
                partitions.append(partition_id)

        search_start = uuid_index + 16

    partitions = sorted(list(set(partitions)))
    print("FINAL PARTITIONS:", partitions)

    if not partitions:
        return [0]

    return partitions


def serialize_partition(partition_index):
    data = b""
    data += (0).to_bytes(2, "big")
    data += partition_index.to_bytes(4, "big")
    data += (1).to_bytes(4, "big")
    data += (0).to_bytes(4, "big")
    data += b"\x02"
    data += (1).to_bytes(4, "big")
    data += b"\x02"
    data += (1).to_bytes(4, "big")
    data += b"\x01"
    data += b"\x01"
    data += b"\x01"
    data += b"\x00"
    return data
