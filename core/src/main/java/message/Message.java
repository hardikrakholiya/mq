package message;

import java.io.Serializable;

public class Message implements Serializable {

    private long timestamp;
    private String text;

    public Message(String text) {
        this.text = text;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getText() {
        return text;
    }
}
