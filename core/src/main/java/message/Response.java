package message;

public class Response {
    private int status;
    private Object data;

    public Response(Object data) {
        this.status = 0;
        this.data = data;
    }

    public Response(int status, String data) {
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
