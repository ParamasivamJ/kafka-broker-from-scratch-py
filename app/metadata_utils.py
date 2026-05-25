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


def find_topic_metadata(
    metadata,
    topic_name
):

    print("\n========== FIND TOPIC ==========")

    print("TOPIC:", topic_name)

    # -------------------------------------------------
    # Compact string encoding
    #
    # length + 1
    # -------------------------------------------------

    encoded_topic = (
        bytes([len(topic_name) + 1]) +
        topic_name
    )

    print("ENCODED TOPIC:")

    print(encoded_topic.hex())

    # -------------------------------------------------
    # Find exact encoded topic
    # -------------------------------------------------

    topic_index = metadata.find(
        encoded_topic
    )

    print("TOPIC INDEX:", topic_index)

    if topic_index == -1:

        print("TOPIC NOT FOUND")

        print("================================\n")

        return None

    # -------------------------------------------------
    # UUID comes AFTER encoded topic
    # -------------------------------------------------

    uuid_start = (
        topic_index +
        len(encoded_topic)
    )

    topic_uuid = metadata[
        uuid_start:
        uuid_start + 16
    ]

    print("UUID:", topic_uuid.hex())

    print("================================\n")

    return {
        "uuid": topic_uuid
    }