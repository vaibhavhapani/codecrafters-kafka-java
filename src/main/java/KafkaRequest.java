import java.util.List;

public class KafkaRequest {
    public final short apiKey;
    public final short apiVersion;
    public final int correlationId;
    public List<String> topicNames;

    public KafkaRequest(short apiKey, short apiVersion, int correlationId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
    }

    // Constructor for DescribeTopicPartitions requests
    public KafkaRequest(short apiKey, short apiVersion, int correlationId, List<String> topicNames) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.topicNames = topicNames;
    }
}
