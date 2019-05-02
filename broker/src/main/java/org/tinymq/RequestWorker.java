package org.tinymq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.json.JSONObject;
import org.tinymq.message.Message;
import org.tinymq.message.Queue;
import org.tinymq.message.Request;
import org.tinymq.message.Response;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.tinymq.Configurations.getHostAddress;
import static org.tinymq.Configurations.getPort;

public class RequestWorker extends Thread {
    private ZContext context;
    private final Map<String, Queue<Message>> topicMsgQMap;
    private final Map<String, Object> ackMonitorMap;
    private ObjectMapper objectMapper = new ObjectMapper();
    private CuratorFramework zkClient;
    private Map<String, PathChildrenCache> zkPathCache;

    public RequestWorker(ZContext context, RequestReceiver master) {
        this.context = context;
        this.topicMsgQMap = master.getTopicMsgQMap();
        this.ackMonitorMap = master.getAckMonitorMap();
        this.zkClient = master.getZKClient();
        this.zkPathCache = master.getZkPathCache();
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
                    System.out.println("put request received");

                    String topic = requestObj.getTopic();
                    Message message = requestObj.getMsg();
                    Queue<Message> msgQ = getMsgQ(topic);
                    msgQ.enqueue(message);

                    final Object monitor = new Object();
                    int lastMessageOffset = msgQ.getOffsetOfLastItem();
                    ackMonitorMap.put(topic + "#" + lastMessageOffset, monitor);

                    System.out.println("requesting acks from followers");

                    System.out.println("followers: " + getFollowerCnxnStrings(topic));

                    for (String followerCnxn : getFollowerCnxnStrings(topic)) {
                        ZMQ.Socket requester = context.createSocket(SocketType.REQ);
                        requester.connect("tcp://" + followerCnxn);
                        Request putAtRequest = new Request();
                        putAtRequest.setOffset(lastMessageOffset);
                        putAtRequest.setTopic(topic);
                        putAtRequest.setType("put_at");
                        putAtRequest.setMsg(message);
                        putAtRequest.setReplyTo(getHostAddress() + ":" + getPort());
                        System.out.println("sending to follower " + followerCnxn + " : " + putAtRequest);
                        requester.send(objectMapper.writeValueAsString(putAtRequest), ZMQ.DONTWAIT);
                    }

                    System.out.println("waiting for acks from followers");
                    synchronized (monitor) {
                        monitor.wait();
                    }

                    System.out.println("sending response to client");
                    // send ack back to client
                    socket.send(objectMapper.writeValueAsString(new Response("")), 0);
                }

                //handle get request
                else if (requestObj.getType().equalsIgnoreCase("get")) {
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Queue<Message> msgQ = getMsgQ(topic);
                    socket.send(objectMapper.writeValueAsString(new Response(msgQ.getItemAtOffset(offset))), 0);
                }

                //put at specific offset
                else if (requestObj.getType().equalsIgnoreCase("put_at")) {
                    System.out.println("put_at request received from master");
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Message message = requestObj.getMsg();

                    Queue<Message> msgQ = getMsgQ(topic);
                    msgQ.putItemAtOffset(message, offset);

                    Request ack = new Request();
                    ack.setType("ack");
                    ack.setTopic(topic);
                    ack.setOffset(offset);
                    System.out.println("sending ack to master" + ack);

                    ZMQ.Socket requester = context.createSocket(SocketType.REQ);
                    requester.connect("tcp://" + requestObj.getReplyTo());
                    requester.send(objectMapper.writeValueAsString(ack));
                }

                //ack sent by followers
                else if (requestObj.getType().equalsIgnoreCase("ack")) {

                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Object monitor = ackMonitorMap.get(topic + "#" + offset);

                    synchronized (monitor) {
                        monitor.notify();
                    }
                }

                //get batch of messages from master broker at a given offset
                else if (requestObj.getType().equalsIgnoreCase("get_batch")) {
                    String topic = requestObj.getTopic();
                    int offset = requestObj.getOffset();
                    Queue<Message> msgQ = getMsgQ(topic);
                    socket.send(SerializationUtils.serialize(msgQ.getBatchUpto(offset)), ZMQ.DONTWAIT);
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

    private Queue<Message> getMsgQ(String topic) throws Exception {
        // check if the topic queue is present, create one if not
        if (!topicMsgQMap.containsKey(topic)) {
            synchronized (topicMsgQMap) {
                if (!topicMsgQMap.containsKey(topic)) {

                    //create topic queue
                    Queue<Message> q = new Queue<>();
                    topicMsgQMap.put(topic, q);

                    return q;
                }
            }
        }

        return topicMsgQMap.get(topic);
    }

    private String getMasterCnxnString(String topic) throws Exception {
//        String masterPath = zkClient.getData().forPath("/topic/" + topic + "/master");
        return null;

    }

    private List<String> getFollowerCnxnStrings(String topic) throws Exception {
        String followersPath = "/topic/" + topic + "/followers";

        List<String> followers = new ArrayList<>();

        for (Object obj : zkClient.getChildren().forPath(followersPath)) {
            JSONObject jsonObject = new JSONObject(new String(zkClient.getData().forPath(followersPath + "/" + obj)));
            followers.add(jsonObject.get("address") + ":" + jsonObject.get("port"));
        }

        return followers;
    }


}