import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class PutTest {
        public static void main(String[] args) {
            try (ZContext context = new ZContext()) {
                //  Socket to talk to server
                ZMQ.Socket requester = context.createSocket(SocketType.REQ);
                requester.connect("tcp://localhost:5555");

                requester.send("{" +
                        "\"type\":\"put\"," +
                        "\"topic\":\"Q1\"," +
                        "\"msg\":{" +
                        "\"text\":\"i will find you, and i will kill you\"," +
                        "\"timestamp\" : 1234567890" +
                        "}" +
                        "}", 0);
                String reply = requester.recvStr(0);
                System.out.println(reply);
            }
        }
}

