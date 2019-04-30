package org.tinymq;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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

import static org.tinymq.Configurations.*;

public class RequestReceiver {

    private ZContext context = new ZContext();
    private final Map<String, Queue<Message>> topicMsgQMap = new HashMap<>();
    private int bindPort;

    public RequestReceiver() {

        //router-dealer for workers
        new Thread(() -> {
            ZMQ.Socket clients = context.createSocket(SocketType.ROUTER);
            bindPort = clients.bindToRandomPort("tcp://*");

            ZMQ.Socket workers = context.createSocket(SocketType.DEALER);
            workers.bind("inproc://workers");

            for (int i = 0; i < getNumWorkers(); i++) {
                new RequestWorker(context, topicMsgQMap).start();
            }

            ZMQ.proxy(clients, workers, null);
        }).start();

        registerThisService();
    }

    private void registerThisService() {
        try {
            CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(getZkCnxnString(), new RetryNTimes(5, 1000));
            curatorFramework.start();

            ServiceInstance<Integer> serviceInstance = ServiceInstance.<Integer>builder()
                    .address(Configurations.getHostAddress())
                    .port(bindPort)
                    .name(getServiceName())
                    .id(getInstanceId())
                    .build();

            ServiceDiscoveryBuilder.builder(Integer.class)
                    .basePath(getZkBasePath())
                    .client(curatorFramework)
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
}