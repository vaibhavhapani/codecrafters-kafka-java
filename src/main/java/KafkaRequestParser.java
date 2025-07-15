import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaRequestParser {
    public static KafkaRequest parse(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);

        short apiKey = buffer.getShort(); // 2 bytes
        short apiVersion = buffer.getShort(); // 2 bytes
        int correlationId = buffer.getInt(); // 4 bytes

        if(apiKey == 75) return parseDescribeTopicPartitionsRequest(buffer, apiKey, apiVersion, correlationId);

        return new KafkaRequest(apiKey, apiVersion, correlationId);
    }

    public static KafkaRequest parseDescribeTopicPartitionsRequest(ByteBuffer buffer, short apiKey, short apiVersion, int correlationId){
        int clientIdLength = buffer.getShort();
        if(clientIdLength > 0) buffer.position(buffer.position() + clientIdLength);

        int topicArrayLength = buffer.get() & 0xFF; // unsigned byte
        topicArrayLength = topicArrayLength - 1;

        System.out.println("********************************************************************************************************************" + topicArrayLength);

        List<String> topicNames = new ArrayList<>();
        for(int i = 0; i < topicArrayLength; i++) {
            int topicNameLength = buffer.get() & 0xFF; // unsigned byte

            byte[] topicNameBytes = new byte[topicNameLength];
            buffer.get(topicNameBytes);
            String topicName = new String(topicNameBytes);
            topicNames.add(topicName);

            buffer.get(); // Skip tag buffer for topic
        }

        return new KafkaRequest(apiKey, apiVersion, correlationId, topicNames);
    }
}
