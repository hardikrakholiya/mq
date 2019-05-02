package org.tinymq;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public abstract class Configurations {
    private static String hostAddress;
    private static String zkCnxnString;
    private static String zkBasePath;
    private static String serviceName;
    private static int numWorkers;
    private static int port;
    private static String id;


    public static void loadConfigurations(String configPath) {
        try {

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(configPath));
            } catch (IOException e) {
                System.out.println("Properties file at path: {" + configPath + "} not found");
                System.exit(-1);
            }

            //zk server details
            String zkAddress = properties.getProperty("zk.address");
            int zkPort = Integer.parseInt(properties.getProperty("zk.port"));
            zkCnxnString = zkAddress + ":" + zkPort;

            //host details
            hostAddress = InetAddress.getLocalHost().getHostAddress();

            //worker details
            numWorkers = Integer.parseInt(properties.getProperty("num_workers"));

            //service registry
            zkBasePath = "/";
            serviceName = "broker";


        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String getZkCnxnString() {
        return zkCnxnString;
    }

    public static String getZkBasePath() {
        return zkBasePath;
    }

    public static String getServiceName() {
        return serviceName;
    }

    public static String getHostAddress() {
        return hostAddress;
    }

    public static int getNumWorkers() {
        return numWorkers;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        Configurations.port = port;
    }

    public static String getID() {
        if (id == null) {
            id = Integer.toHexString((hostAddress + ":" + port).hashCode());
        }
        return id;
    }
}
