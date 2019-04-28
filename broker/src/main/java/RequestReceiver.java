import message.Message;
import message.Queue;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

public class RequestReceiver {

    private ZContext context = new ZContext();
    private final Map<String, Queue<Message>> topicMsgQMap = new HashMap<>();

    public RequestReceiver(int pushPort, int getPort, int numWorkers) {

        //router-dealer for publishers
        new Thread(() -> {
            ZMQ.Socket publishers = context.createSocket(SocketType.ROUTER);
            publishers.bind("tcp://*:" + pushPort);

            ZMQ.Socket pushers = context.createSocket(SocketType.DEALER);
            pushers.bind("inproc://pushers");

            for (int i = 0; i < numWorkers; i++) {
                new MessagePusher(context, topicMsgQMap).start();
            }

            ZMQ.proxy(publishers, pushers, null);
        }).start();

        //router-dealer for subscribers
        new Thread(() -> {

            ZMQ.Socket subscribers = context.createSocket(SocketType.ROUTER);
            subscribers.bind("tcp://*:" + getPort);

            ZMQ.Socket getters = context.createSocket(SocketType.DEALER);
            getters.bind("inproc://getters");

            for (int i = 0; i < numWorkers; i++) {
                new MessageGetter(context, topicMsgQMap).start();
            }

            ZMQ.proxy(subscribers, getters, null);
        }).start();
    }

    public static void main(String[] args) {
        new RequestReceiver(5555, 5556, 5);
    }
}