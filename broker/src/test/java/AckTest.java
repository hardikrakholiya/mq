import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class AckTest {
    public static void main(String[] args) throws InterruptedException {
        try (ZContext context = new ZContext()) {
            //  Socket to talk to server
            ZMQ.Socket requester = context.createSocket(SocketType.REQ);
            requester.connect("tcp://localhost:5555");

            String request = "{" +
                    "\"type\":\"ack\"," +
                    "\"topic\":\"Q1\"," +
                    "\"offset\":" + 1 +
                    "}";
            requester.send(request, ZMQ.DONTWAIT);
            Thread.sleep(1000);
        }
    }
}
