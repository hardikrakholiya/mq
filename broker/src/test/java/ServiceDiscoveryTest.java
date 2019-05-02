import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class ServiceDiscoveryTest {

    private static CuratorFramework client = CuratorFrameworkFactory.newClient("149.160.181.121:2181", new RetryNTimes(5, 1000));

    public static void main(String[] args) throws Exception {
        //connect to ZK
        client.start();

        for (Object obj : client.getChildren().forPath("/topic/q1/followers")) {
            System.out.println(obj);
            System.out.println(new String(client.getData().forPath("/topic/q1/followers/" + obj)))
            ;
        }


        System.out.println();
//        For (){
//
//        };

        /*PathChildrenCache cache = new PathChildrenCache(client, "/topic/q1/master", true);
        cache.start();
        cache.rebuild();

        new Thread(() -> {
            try {
                Thread.sleep(5000);

                String str = "000000000000000000";
                client.setData().forPath("/topic/q1/master/b2", str.getBytes());

//                cache.rebuild();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        while (true) {
            for (ChildData childData : cache.getCurrentData()) {
                System.out.println(childData.getPath());
                System.out.println(Arrays.toString(childData.getData()));

            }

            Thread.sleep(1000);
        }*/


    }
}
