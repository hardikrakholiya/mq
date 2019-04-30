package org.tinymq.message;

public class Response {
    private int status;
    private Object data;

    public Response() {
        this(0, null);
    }

    public Response(Object data) {
        this(0, data);
    }

    public Response(int status, Object data) {
        this.status = status;
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public Object getData() {
        return data;
    }
}
