import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

public class SimpleZookeeperClientTest {
    String connect = "192.168.229.129:2181,192.168.229.130:2181,192.168.229.131:2181";
    int timeout = 2000;
    ZooKeeper zk = null;

    @Before
    public void init() throws Exception {
        zk = new ZooKeeper(connect, timeout, we -> {
            System.out.println("- - - watcher - - - \ntype is "+we.getType()+"\nstate is "+we.getState()+"\npath is "+we.getPath()+"\n- - - watcher - - - ");
            try {
                zk.getChildren("/", true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void createNode() throws Exception {
        zk.create("/idea", "node".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void isExist() throws Exception{
        Stat stat = zk.exists("/idea",true);
        System.out.println(Objects.nonNull(stat));
    }

    @Test
    public void getNode() throws Exception {
        List<String> list = zk.getChildren("/",true);
        list.stream().forEach(System.out::println);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getNodeData() throws Exception {
        byte[] bytes = zk.getData("/idea",false, new Stat());
        System.out.println("data = " + new String(bytes));
    }


}
