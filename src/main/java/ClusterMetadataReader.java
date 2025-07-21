import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
            int currentBatchLength = buffer.getInt(); // Batch Length
            int currentBatchEnd = buffer.position() + currentBatchLength;

            buffer.position(buffer.position() + 49); // skipp unnecessary fields for now

            int records = buffer.getInt(); // Records Length -> 4 bytes

            for (int record = 0; record < records; record++) {
                int length = buffer.get();
                int recordEnd = buffer.position() + length;
                System.out.println("length: " + length);

                int attributes = buffer.get();
                System.out.println("Attributes: " + attributes);

                byte timeStampDeltaSignedByte = buffer.get();
                int timeStampDelta = timeStampDeltaSignedByte;
                System.out.println("Timestamp Delta signed byte and int: " + timeStampDeltaSignedByte + " " + timeStampDelta);

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
                System.out.println("Frame Version: " + frameVersion);

                int type = buffer.get() & 0xFF;
                System.out.println("Type: " + type);

                switch (type) {
                    case KafkaConstants.FEATURE_LEVEL_RECORD:
                        buffer.position(recordEnd);
                        break;
                    case KafkaConstants.TOPIC_RECORD:
                        int topicRecordVersion = buffer.get() & 0xFF;
                        System.out.println("Version: " + topicRecordVersion);

                        int topicNameLength = buffer.get() & 0xFF;
                        topicNameLength = topicNameLength - 1;
                        System.out.println("Name Length: " + topicNameLength);

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

                        if(topicId != null && topicId == partitionTopicUUID) {
                            partitions.add(partitionId);
                        }

                        buffer.position(recordEnd);

                        break;
                    default:
                        System.out.println("Unknown record type: " + type);
                        buffer.position(recordEnd);
                        break;
                }
            }
            buffer.position(currentBatchEnd);
        }
        if(foundTopicName != null) return new TopicMetadata(foundTopicName, topicId, partitions);

        return null;
    }
}
