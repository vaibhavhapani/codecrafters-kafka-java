public class KafkaRequest {
    public final short apiKey;
    public final short apiVersion;
    public final int correlationId;

    public KafkaRequest(short apiKey, short apiVersion, int correlationId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
    }
}
