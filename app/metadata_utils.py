METADATA_PATH = (
    "/tmp/kraft-combined-logs/"
    "__cluster_metadata-0/"
    "00000000000000000000.log"
)


def load_metadata():
    print("\n========== LOAD METADATA ==========")
    print("METADATA PATH:", METADATA_PATH)

    with open(METADATA_PATH, "rb") as f:
        data = f.read()

    print("METADATA SIZE:", len(data))
    print("===================================\n")
    return data


def extract_partitions(metadata, topic_uuid):
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


def find_topic_metadata(metadata, topic_name):
    topic_index = metadata.find(topic_name)

    if topic_index != -1:
        print("TOPIC FOUND:", topic_name)
        print("TOPIC INDEX:", topic_index)

        start = max(0, topic_index - 64)
        end = topic_index + 64
        print(metadata[start:end].hex())

        # Keep the same logic as the current implementation
        uuid_start = topic_index + len(topic_name)

        topic_uuid = metadata[uuid_start:uuid_start + 16]
        partitions = extract_partitions(metadata, topic_uuid)
        error_code = 0
    else:
        print("TOPIC NOT FOUND:", topic_name)
        topic_uuid = b"\x00" * 16
        partitions = []
        error_code = 3

    return error_code, topic_uuid, partitions