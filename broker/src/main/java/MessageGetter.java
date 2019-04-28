import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import message.GetRequest;
import message.Message;
import message.Queue;
import message.Response;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;

public class MessageGetter extends Thread {

    private final ZContext context;
    private final Map<String, Queue<Message>> topicMsgQMap;
    private ObjectMapper objectMapper = new ObjectMapper();

    public MessageGetter(ZContext context, Map<String, Queue<Message>> topicMsgQMap) {
        this.context = context;
        this.topicMsgQMap = topicMsgQMap;
    }

    public void run() {
        ZMQ.Socket socket = context.createSocket(SocketType.REP);
        socket.connect("inproc://getters");

        while (!Thread.currentThread().isInterrupted()) {
            // wait for next request from client
            String request = socket.recvStr(0);
            System.out.println(Thread.currentThread().getName() + " Received request: [" + request + "]");

            try {
                // parse json object from request
                ObjectMapper objectMapper = new ObjectMapper();


                GetRequest getRequest = objectMapper.readValue(request, GetRequest.class);
                String topic = getRequest.getTopic();
                int offset = getRequest.getOffset();
                Queue<Message> msgQ = topicMsgQMap.get(topic);
                socket.send(objectMapper.writeValueAsString(new Response(msgQ.atOffset(offset))), 0);

            } catch (Exception e) {
                try {
                    socket.send(objectMapper.writeValueAsString(new Response(1, e.getMessage())), 0);
                } catch (JsonProcessingException e1) {
                    e1.printStackTrace();
                }
            }


        }
    }
}
