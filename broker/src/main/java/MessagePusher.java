import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;
import message.PushRequest;
import message.Queue;
import message.Response;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;

public class MessagePusher extends Thread {
    private ZContext context;
    private final Map<String, Queue<Message>> topicMsgQMap;
    private ObjectMapper objectMapper = new ObjectMapper();

    public MessagePusher(ZContext context, Map<String, Queue<Message>> topicMsgQMap) {
        this.context = context;
        this.topicMsgQMap = topicMsgQMap;
    }

    @Override
    public void run() {
        ZMQ.Socket socket = context.createSocket(SocketType.REP);
        socket.connect("inproc://pushers");

        while (!Thread.currentThread().isInterrupted()) {
            // wait for next request from client
            String request = socket.recvStr(0);
            System.out.println(Thread.currentThread().getName() + " Received request: [" + request + "]");

            try {
                // parse json object from request
                PushRequest pushRequest = objectMapper.readValue(request, PushRequest.class);

                String topic = pushRequest.getTopic();
                Message message = pushRequest.getMsg();

                // check if the topic queue is present, create one if not
                if (!topicMsgQMap.containsKey(topic)) {
                    synchronized (topicMsgQMap) {
                        if (!topicMsgQMap.containsKey(topic)) {
                            System.out.println("creating queue for: " + topic);
                            topicMsgQMap.put(topic, new Queue<>());
                        }
                    }
                }

                Queue<Message> msgQ = topicMsgQMap.get(topic);
                msgQ.enqueue(message);

                // send ack back to client
                socket.send(objectMapper.writeValueAsString(new Response("")), 0);
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