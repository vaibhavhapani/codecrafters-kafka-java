import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaRequestParser {
    public static KafkaRequest parse(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);

        // Read the header
        short apiKey = buffer.getShort(); // 2 bytes
        short apiVersion = buffer.getShort(); // 2 bytes
        int correlationId = buffer.getInt(); // 4 bytes
        int clientIdLength = buffer.getShort(); // 2 bytes

        if (clientIdLength > 0) {
            buffer.position(buffer.position() + clientIdLength);
        }

        buffer.get(); // skip tag buffer

        // parse request body based on apiKey
        if (apiKey == 75) return parseDescribeTopicPartitionsRequest(buffer, apiKey, apiVersion, correlationId);

        return new KafkaRequest(apiKey, apiVersion, correlationId);
    }

    public static KafkaRequest parseDescribeTopicPartitionsRequest(ByteBuffer buffer, short apiKey, short apiVersion, int correlationId) {
        int topicArrayLength = buffer.get() & 0xFF; // unsigned byte
        topicArrayLength = topicArrayLength - 1;

        System.out.println("Request: Topic array length is: " + topicArrayLength);

        List<String> topicNames = new ArrayList<>();
        for (int i = 0; i < topicArrayLength; i++) {
            int topicNameLength = buffer.get() & 0xFF; // unsigned byte
            topicNameLength = topicNameLength - 1;

            System.out.println("Request: Topic name length is: " + topicNameLength);

            byte[] topicNameBytes = new byte[topicNameLength];
            buffer.get(topicNameBytes);
            String topicName = new String(topicNameBytes);

            topicNames.add(topicName);

            System.out.println("Request: Topic name is: " + topicName);

            buffer.get(); // Skip tag buffer for topic
        }

        // Parse response_partition_limit (int32)
        int responsePartitionLimit = buffer.getInt();

        // Parse cursor (nullable bytes)
        byte cursorLength = buffer.get();
        if (cursorLength > 0) {
            buffer.position(buffer.position() + cursorLength - 1);
        }

        System.out.println("\n***************** Request Parsed *********************\n");

        return new KafkaRequest(apiKey, apiVersion, correlationId, topicNames);
    }
}
