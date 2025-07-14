import java.nio.ByteBuffer;

public class KafkaRequestParser {
    public static KafkaRequest parse(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);

        short apiKey = buffer.getShort(); // 2 bytes
        short apiVersion = buffer.getShort(); // 2 bytes
        int correlationId = buffer.getInt(); // 4 bytes

        return new KafkaRequest(apiKey, apiVersion, correlationId);
    }
}
