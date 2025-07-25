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

        int i = 0;
        while (buffer.remaining() > 0) {
            System.out.println("\n********************** Batch " + (i + 1) + " starts at " + buffer.position() + " ************************\n");

            long baseOffset = buffer.getLong(); // Base Offset
            int batchLength = buffer.getInt(); // Batch Length

            if (batchLength <= 0 || batchLength > buffer.remaining()) {
                System.out.println("Invalid batch length: " + batchLength + ", remaining: " + buffer.remaining());
                break;
            }

            System.out.println("\nBatch Length: " + batchLength);

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
            System.out.println("Processing batch with " + recordsCount + " records\n");


            for (int record = 0; record < recordsCount && buffer.position() < batchEnd; record++) {
                if (buffer.remaining() < 1) break;
                System.out.println("*********** Record " + (record + 1) + " ***********" + "\nRecord Start: " + buffer.position());

                int recordLength = readVarint(buffer);
                int recordEnd = buffer.position() + recordLength;
                System.out.println("Record Length: " + recordLength + "\nRecord End: " + recordEnd);

                if (recordLength <= 0 || recordEnd > batchEnd) {
                    System.out.println("Invalid record length: " + recordLength + ", remaining: " + (batchEnd - buffer.position()));
                    buffer.position(batchEnd);
                    break;
                }

                int attributes = buffer.get() & 0xFF;
                int timeStampDelta = readVarint(buffer);
                int offsetDelta = readVarint(buffer);
                int keyLength = readVarint(buffer);

                System.out.println("Attributes: " + attributes + "\nTimestamp Delta: " + timeStampDelta + "\nOffset Delta: " + offsetDelta + "\nKey Length: " + keyLength);

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

                int valueLength = readVarint(buffer);
                System.out.println("Value length: " + valueLength);

                if (valueLength <= 0) {
                    System.out.println("Skipping record with invalid value length: " + valueLength);
                    buffer.position(recordEnd);
                    continue;
                }

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

                        System.out.println("Topic UUID: " + bytesToHex(topicUUID));

                        if (topicName.equals(currentTopicName)) {
                            foundTopicName = currentTopicName;
                            topicId = topicUUID;
                        }

                        int taggedFieldsCount = buffer.get() & 0xFF;
                        int headersArrayCount = buffer.get() & 0xFF;

                        System.out.println(buffer.position());

                        buffer.position(recordEnd);
                        break;

                    case KafkaConstants.PARTITION_RECORD:
                        int partitionRecordVersion = buffer.get() & 0xFF;
                        int partitionId = buffer.getInt();

                        System.out.println("Version: " + partitionRecordVersion + "\nPartition Id: " + partitionId);

                        byte[] partitionTopicUUID = new byte[16];
                        buffer.get(partitionTopicUUID);

                        System.out.println("Partition topic UUID: " + bytesToHex(partitionTopicUUID));

                        if (topicId != null && Arrays.equals(topicId, partitionTopicUUID)) {
                            System.out.printf("MATCH! Adding partition %d to topic %s%n", partitionId, foundTopicName);
                            partitions.add(partitionId);
                        }

                        buffer.position(recordEnd);
                        break;
                    default:
                        System.out.println("Skipping record type: " + type);
                        buffer.position(recordEnd);
                        break;
                }

                // Uncomment the lines below to inspect raw bytes
//                if(topicId != null) {
//                    int start = buffer.position();
//                    byte[] b = new byte[buffer.remaining()];
//                    buffer.get(b);
//                    buffer.position(start);
//                    System.out.println(bytesToHex(b));
//                }

                System.out.println();
            }
            buffer.position(batchEnd);
            System.out.println("\n********************** Batch " + (i + 1) + " Over at " + (buffer.position() - 1) + " ************************\n");
            i++;
        }

        if (foundTopicName != null) {
            System.out.println("Final result - Topic: " + foundTopicName + ", Partitions: " + partitions.size());
            analyzeUuidOccurrences(logs, topicId);
            return new TopicMetadata(foundTopicName, topicId, partitions);
        }

        return null;
    }

    public static int readVarint(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            b = buffer.get();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return (value >>> 1) ^ -(value & 1); // ZigZag decode
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x ", b));
        }
        return result.toString();
    }

    // For debugging purposes
    private static void analyzeUuidOccurrences(byte[] fileBytes, byte[] targetUuid) {
        String fileContent = bytesToHex(fileBytes);
        String uuidStr = bytesToHex(targetUuid).replace(" ", "");

        System.out.println("\n===== UUID ANALYSIS =====");
        System.out.println("Searching for: " + uuidStr);

        int index = -1;
        int count = 0;
        while ((index = fileContent.indexOf(uuidStr, index + 1)) != -1) {
            count++;
            System.out.println("Found at position: " + (index / 2)); // /2 because hex representation
        }
        System.out.println("Total occurrences: " + count);
        System.out.println("=======================\n");
    }
}
