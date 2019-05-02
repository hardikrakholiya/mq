package org.tinymq;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.tinymq.message.Message;
import org.tinymq.message.Queue;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.tinymq.Configurations.*;

public class RequestReceiver {

    private ZContext context = new ZContext();
    private final Map<String, Queue<Message>> topicMsgQMap = new HashMap<>();
    private final Map<String, Object> ackMonitorMap = new ConcurrentHashMap<>();
    private final Map<String, PathChildrenCache> zkPathCache = new ConcurrentHashMap<>();
    private CuratorFramework client = CuratorFrameworkFactory.newClient(getZkCnxnString(), new RetryNTimes(5, 1000));

    public RequestReceiver() {

        //router-dealer for workers
        new Thread(() -> {
            ZMQ.Socket clients = context.createSocket(SocketType.ROUTER);
            setPort(clients.bindToRandomPort("tcp://*"));

            ZMQ.Socket workers = context.createSocket(SocketType.DEALER);
            workers.bind("inproc://workers");

            for (int i = 0; i < getNumWorkers(); i++) {
                new RequestWorker(context, this).start();
            }

            ZMQ.proxy(clients, workers, null);

        }).start();

        //connect to ZK
        client.start();

        try {
            //wait for this service to bind to a port
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        registerThisService();

    }

    private void registerThisService() {
        try {
            ServiceInstance<Object> serviceInstance = ServiceInstance.<Object>builder()
                    .address(getHostAddress())
                    .port(getPort())
                    .name(getServiceName())
                    .id("b_" + getID())
                    .build();

            ServiceDiscoveryBuilder.builder(Object.class)
                    .basePath(getZkBasePath())
                    .client(client)
                    .thisInstance(serviceInstance)
                    .build()
                    .start();

        } catch (Exception e) {
            System.out.println("Failed to register the instance to service registry\n" + e.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Please provide path to config.properties as an argument");
            System.exit(1);
        }

        Configurations.loadConfigurations(args[0]);

        new RequestReceiver();
    }

    public Map<String, Queue<Message>> getTopicMsgQMap() {
        return topicMsgQMap;
    }

    public Map<String, Object> getAckMonitorMap() {
        return ackMonitorMap;
    }

    public CuratorFramework getZKClient() {
        return client;
    }

    public Map<String, PathChildrenCache> getZkPathCache() {
        return zkPathCache;
    }
}