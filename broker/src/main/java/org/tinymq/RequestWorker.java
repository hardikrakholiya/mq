package org.tinymq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.tinymq.message.Message;
import org.tinymq.message.Queue;
import org.tinymq.message.Request;
import org.tinymq.message.Response;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;

public class RequestWorker extends Thread {
    private ZContext context;
    private final Map<String, Queue<Message>> topicMsgQMap;
    private ObjectMapper objectMapper = new ObjectMapper();

    public RequestWorker(ZContext context, Map<String, Queue<Message>> topicMsgQMap) {
        this.context = context;
        this.topicMsgQMap = topicMsgQMap;
    }

    @Override
    public void run() {
        ZMQ.Socket socket = context.createSocket(SocketType.REP);
        socket.connect("inproc://workers");

        while (!Thread.currentThread().isInterrupted()) {
            // wait for next request from client
            String request = socket.recvStr(0);
            System.out.println(Thread.currentThread().getName() + " Received request: [" + request + "]");

            try {
                // parse json object from request
                Request requestObj = objectMapper.readValue(request, Request.class);

                //handle put request
                if (requestObj.getType().equalsIgnoreCase("put")) {
                    String topic = requestObj.getTopic();
                    Message message = requestObj.getMsg();

                    Queue<Message> msgQ = getMsgQ(topic);

                    msgQ.enqueue(message);

                    // send ack back to client
                    socket.send(objectMapper.writeValueAsString(new Response("")), 0);
                }

                //handle get request
                else if (requestObj.getType().equalsIgnoreCase("get")) {
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Queue<Message> msgQ = getMsgQ(topic);
                    socket.send(objectMapper.writeValueAsString(new Response(msgQ.getItemAtOffset(offset))), ZMQ.DONTWAIT);
                }

                //put at specific offset
                else if (requestObj.getType().equalsIgnoreCase("put_at")) {
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Message message = requestObj.getMsg();

                    Queue<Message> msgQ = getMsgQ(topic);
                    msgQ.putItemAtOffset(message, offset);
                    //todo send ack
                }

                //
                else if (requestObj.getType().equalsIgnoreCase("ack")) {

                }

                //get batch of messages from master broker at a given offset
                else if (requestObj.getType().equalsIgnoreCase("get_batch")) {
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Queue<Message> msgQ = getMsgQ(topic);
                    socket.send(SerializationUtils.serialize(msgQ.getBatchAtOffset(offset)), ZMQ.DONTWAIT);
                }
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    socket.send(objectMapper.writeValueAsString(new Response(1, e.getMessage())), ZMQ.DONTWAIT);
                } catch (JsonProcessingException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private Queue<Message> getMsgQ(String topic) {
        // check if the topic queue is present, create one if not
        if (!topicMsgQMap.containsKey(topic)) {
            synchronized (topicMsgQMap) {
                if (!topicMsgQMap.containsKey(topic)) {
                    Queue<Message> q = new Queue<>();
                    topicMsgQMap.put(topic, q);
                    return q;
                }
            }
        }

        return topicMsgQMap.get(topic);
    }
}