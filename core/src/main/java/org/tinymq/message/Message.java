package org.tinymq.message;

import java.io.Serializable;

public class Message implements Serializable {

    private long timestamp;
    private String text;

    public long getTimestamp() {
        return timestamp;
    }

    public String getText() {
        return text;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setText(String text) {
        this.text = text;
    }
}
