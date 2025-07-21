import java.util.List;

public class TopicMetadata {
    public final String topicName;
    public final byte[] topicId;
    public final List<Integer> partitions;

    public TopicMetadata(String topicName, byte[] topicId, List<Integer> partitions){
        this.topicName = topicName;
        this.topicId = topicId;
        this.partitions = partitions;
    }
}
