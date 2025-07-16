import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class KafkaResponseBuilder {

    public static byte[] buildResponse(KafkaRequest request) throws IOException {
        if (request.apiKey == 18) return buildApiVersionsResponse(request);
        else if (request.apiKey == 75) return buildDescribeTopicPartitionsResponse(request);

        throw new IllegalArgumentException("Unsupported API Key: " + request.apiKey);
    }

    public static byte[] buildApiVersionsResponse(KafkaRequest request) throws IOException {
        ByteArrayOutputStream res = new ByteArrayOutputStream();

        // Correlation ID
        res.write(ByteBuffer.allocate(KafkaConstants.INT32_SIZE).putInt(request.correlationId).array());

        short errorCode = (request.apiVersion < KafkaConstants.API_VERSIONS_MIN_VERSION ||
                request.apiVersion > KafkaConstants.API_VERSIONS_MAX_VERSION) ?
                KafkaConstants.UNSUPPORTED_VERSION : KafkaConstants.ERROR_NONE;
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(errorCode).array());

        res.write((byte) 3); // compact array length (actual length + 1)

        // API Key 18: ApiVersions
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.API_VERSIONS).array());
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.API_VERSIONS_MIN_VERSION).array());
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.API_VERSIONS_MAX_VERSION).array());
        res.write(KafkaConstants.EMPTY_TAG_BUFFER);

        // API Key 75: DescribeTopicPartitions
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.DESCRIBE_TOPIC_PARTITIONS).array());
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION).array());
        res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION).array());
        res.write(KafkaConstants.EMPTY_TAG_BUFFER);

        // Throttle time (int32)
        res.write(ByteBuffer.allocate(KafkaConstants.INT32_SIZE).putInt(KafkaConstants.DEFAULT_THROTTLE_TIME_MS).array());

        // Tag buffer
        res.write(KafkaConstants.EMPTY_TAG_BUFFER);

        return res.toByteArray();
    }

    public static byte[] buildDescribeTopicPartitionsResponse(KafkaRequest request) throws IOException {
        ByteArrayOutputStream res = new ByteArrayOutputStream();

        res.write(ByteBuffer.allocate(KafkaConstants.INT32_SIZE).putInt(request.correlationId).array());

        // Tag buffer for response header (required for v0)
        res.write((byte) 0);

        // Throttle time
        res.write(ByteBuffer.allocate(KafkaConstants.INT32_SIZE).putInt(0).array());

        List<String> topicNames = request.topicNames;
        if (topicNames != null && !topicNames.isEmpty()) {
            res.write((byte) (topicNames.size() + 1)); // compact array length (1 byte)

            for (String topicName : topicNames) {
                // 1. Error Code
                res.write(ByteBuffer.allocate(KafkaConstants.INT16_SIZE).putShort(KafkaConstants.UNKNOWN_TOPIC_OR_PARTITION).array());

                byte[] topicNameBytes = topicName.getBytes(); // topic name as a compact string

                // 2. Topic Name Length
                res.write((byte) (topicNameBytes.length + 1));

                // 3. Topic Name content
                res.write(topicNameBytes);

                // 4. Topic ID (UUID) - 00000000-0000-0000-0000-000000000000
                res.write(new byte[16]); // all zeros for unknown topic

                // 5. Is internal (boolean)
                res.write((byte) 0);

                // 6. Partitions array (compact array) - empty for unknown topic
                res.write((byte) 1);

                // 7. Topic authorized operations (int32)
                res.write(ByteBuffer.allocate(KafkaConstants.INT32_SIZE).putInt(Integer.MIN_VALUE).array());

                // 8. Tag buffer
                res.write((byte) 0);
            }
        } else {
            res.write((byte) 1);
        }

        // Next cursor (nullable bytes) - null
        res.write(0xff); // A nullable field that can be used for pagination.

        // Tag buffer
        res.write((byte) 0);

        return res.toByteArray();
    }
}
