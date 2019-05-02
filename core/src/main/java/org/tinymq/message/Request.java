package org.tinymq.message;

public class Request {
    private String type;
    private String topic;
    private int offset;
    private Message msg;
    private String replyTo;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Message getMsg() {
        return msg;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    @Override
    public String toString() {
        return "Request{" +
                "type='" + type + '\'' +
                ", topic='" + topic + '\'' +
                ", offset=" + offset +
                ", msg=" + msg +
                ", replyTo='" + replyTo + '\'' +
                '}';
    }
}
