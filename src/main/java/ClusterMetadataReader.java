import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterMetadataReader {
    public static TopicMetadata readTopicMetadata(String topicName) throws IOException {
        Path logFilePath = Paths.get("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
        byte[] logs = Files.readAllBytes(logFilePath);
        ByteBuffer buffer = ByteBuffer.wrap(logs);

        String foundTopicName = null;
        byte[] topicId = null;
        List<Integer> partitions = new ArrayList<>();

        while (buffer.remaining() > 0) {
            long baseOffset = buffer.getLong(); // Base Offset
            int batchLength = buffer.getInt(); // Batch Length

            if (batchLength <= 0 || batchLength > buffer.remaining()) {
                System.out.println("Invalid batch length: " + batchLength + ", remaining: " + buffer.remaining());
                break;
            }

            int batchEnd = buffer.position() + batchLength;

            // Skip batch header fields we don't need
            buffer.getInt(); // partition leader epoch (4 bytes)
            buffer.get();    // magic byte (1 byte)
            buffer.getInt(); // crc (4 bytes)
            buffer.getShort(); // attributes (2 bytes)
            buffer.getInt(); // last offset delta (4 bytes)
            buffer.getLong(); // base timestamp (8 bytes)
            buffer.getLong(); // max timestamp (8 bytes)
            buffer.getLong(); // producer id (8 bytes)
            buffer.getShort(); // producer epoch (2 bytes)
            buffer.getInt(); // base sequence (4 bytes)

            int recordsCount = buffer.getInt(); // Records Length -> 4 bytes
            System.out.println("Processing batch with " + recordsCount + " records");


            for (int record = 0; record < recordsCount && buffer.position() < batchEnd; record++) {
                if (buffer.remaining() < 1) break;

                int recordLength = buffer.get();
                int recordEnd = buffer.position() + recordLength;
                System.out.println("length: " + recordLength);

                if (recordEnd > batchEnd || recordLength < 0) {
                    System.out.println("Invalid record length: " + recordLength);
                    break;
                }

                // attributes
                int attributes = buffer.get();
                System.out.println("Attributes: " + attributes);

                // timestamp delta
                byte timeStampDeltaSignedByte = buffer.get();
                int timeStampDelta = timeStampDeltaSignedByte;
                System.out.println("Timestamp Delta signed byte and int: " + timeStampDeltaSignedByte + " " + timeStampDelta);

                // offset delta
                byte offsetDeltaSignedByte = buffer.get();
                int offsetDelta = offsetDeltaSignedByte;
                System.out.println("Offset Delta signed byte and int: " + offsetDeltaSignedByte + " " + offsetDelta);

                byte keyLengthSignedByte = buffer.get();
                int keyLength = keyLengthSignedByte;
                if (keyLength != -1) keyLength = keyLengthSignedByte & 0xFF;
                System.out.println("Key Length: " + keyLength);

                byte[] keyBytes;
                String key = null;
                if (keyLength == -1) {
                    System.out.println("Key: null");
                } else {
                    keyBytes = new byte[keyLength];
                    buffer.get(keyBytes);
                    key = new String(keyBytes);
                    System.out.println("Key: " + key);
                }

                byte valueLengthSignedByte = buffer.get();
                int valueLength = valueLengthSignedByte;
                System.out.println("Value length: " + valueLength);

                // Value
                int frameVersion = buffer.get() & 0xFF;
                int type = buffer.get() & 0xFF;

                System.out.println("Frame Version: " + frameVersion + "\nType: " + type);

                switch (type) {
                    case KafkaConstants.TOPIC_RECORD:
                        int topicRecordVersion = buffer.get() & 0xFF;
                        int topicNameLength = buffer.get() & 0xFF;
                        topicNameLength = topicNameLength - 1;

                        System.out.println("Version: " + topicRecordVersion + "\nName Length: " + topicNameLength);

                        byte[] topicNameBytes = new byte[topicNameLength];
                        buffer.get(topicNameBytes);
                        String currentTopicName = new String(topicNameBytes);

                        System.out.println("Topic Name: " + currentTopicName);

                        byte[] topicUUID = new byte[16];
                        buffer.get(topicUUID);

                        if(topicName.equals(currentTopicName)) {
                            foundTopicName = currentTopicName;
                            topicId = topicUUID;
                        }

                        int taggedFieldsCount = buffer.get() & 0xFF;
                        int headersArrayCount = buffer.get() & 0xFF;

                        break;

                    case KafkaConstants.PARTITION_RECORD:
                        int partitionRecordVersion = buffer.get() & 0xFF;
                        System.out.println("Version: " + partitionRecordVersion);

                        int partitionId = buffer.getInt();
                        System.out.println("Partition Id: " + partitionId);

                        byte[] partitionTopicUUID = new byte[16];
                        buffer.get(partitionTopicUUID);

                        if(topicId != null && Arrays.equals(topicId, partitionTopicUUID)) {
                            partitions.add(partitionId);
                        }

                        buffer.position(recordEnd);

                        break;
                    default:
                        System.out.println("Skipping record type: " + type);
                        buffer.position(recordEnd);
                        break;
                }
            }
            buffer.position(batchEnd);
        }
        if(foundTopicName != null) return new TopicMetadata(foundTopicName, topicId, partitions);

        return null;
    }
}
