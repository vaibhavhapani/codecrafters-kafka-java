public final class KafkaConstants {

    // Prevent instantiation
    private KafkaConstants() {}

    // Server configuration
    public static final int PORT = 9092;

    // API Keys
    public static final short API_VERSIONS = 18; // 0x00, 0x12
    public static final short DESCRIBE_TOPIC_PARTITIONS = 75; // 0x00, 0x4B

    // Error codes
    public static final short ERROR_NONE = 0; // 0x00, 0x00
    public static final short UNSUPPORTED_VERSION = 35; // 0x00, 0x23
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;

    // API Version ranges
    public static final short API_VERSIONS_MIN_VERSION = 0; // 0x00, 0x00
    public static final short API_VERSIONS_MAX_VERSION = 4; // 0x00, 0x04

    public static final short DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION = 0; // 0x00, 0x00
    public static final short DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION = 0; // 0x00, 0x00

    // Kafka response defaults
    public static final int DEFAULT_THROTTLE_TIME_MS = 0; // 0x00, 0x00, 0x00, 0x00

    // Kafka protocol field sizes
    public static final int INT32_SIZE = 4;
    public static final int INT16_SIZE = 2;

    // Tag buffer
    public static final byte EMPTY_TAG_BUFFER = 0x00;
}
