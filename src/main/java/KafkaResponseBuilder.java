import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KafkaResponseBuilder {

    public static byte[] buildApiVersionsResponse(KafkaRequest request) throws IOException {
        ByteArrayOutputStream res = new ByteArrayOutputStream();

        res.write(ByteBuffer.allocate(4).putInt(request.correlationId).array());

        short errorCode = (request.apiVersion < 0 || request.apiVersion > 4) ? (short) 35 : (short) 0;
        res.write(ByteBuffer.allocate(2).putShort(errorCode).array());

        res.write(new byte[] {
                4,           // Compact array length = 2 entries + 1 terminator

                // API Key 18: ApiVersions
                0x00, 0x12,     // API Key = 18
                0x00, 0x00,     // Min Version = 0
                0x00, 0x04,     // Max Version = 4
                0x00,           // Tag buffer

                // API Key 75: DescribeTopicPartitions
                0x00, 0x4B,     // API Key = 75
                0x00, 0x00,     // Min Version = 0
                0x00, 0x00,     // Max Version = 0
                0x00,           // Tag buffer

                0x00, 0x00, 0x00, 0x00, // throttle time
                0x00         // tag buffer
        });

        return res.toByteArray();
    }
}
