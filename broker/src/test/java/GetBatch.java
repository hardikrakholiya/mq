import org.apache.commons.lang3.SerializationUtils;
import org.tinymq.message.Message;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class GetBatch {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            // Socket to talk to server
            ZMQ.Socket requester = context.createSocket(SocketType.REQ);
            requester.connect("tcp://localhost:51820");

            System.out.println("launch and connect client.");

            requester.send("{" +
                    "\"type\": \"get_batch\"," +
                    "\"topic\": \"Q1\"," +
                    "\"offset\":  " + 0 + " " +
                    "}", 0);

            Object[] object = SerializationUtils.deserialize(requester.recv(0));
            System.out.println((Message) object[0]);

        }
    }
}
