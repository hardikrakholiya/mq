package message;

public class PushRequest {
    private String topic;
    private Message msg;

    public String getTopic() {
        return topic;
    }

    public Message getMsg() {
        return msg;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }
}
