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
                2,           // compact array length
                0x00, 0x12,  // api_key = 18
                0x00, 0x00,  // min_version = 0
                0x00, 0x04,  // max_version = 4
                0x00,        // tag buffer = empty
                0x00, 0x00, 0x00, 0x00, // throttle time
                0x00         // tag buffer
        });

        return res.toByteArray();
    }
}
