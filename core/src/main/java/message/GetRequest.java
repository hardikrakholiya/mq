package message;

public class GetRequest {
    private String topic;
    private int offset;

    public String getTopic() {
        return topic;
    }

    public int getOffset() {
        return offset;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
