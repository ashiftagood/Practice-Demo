package com.zhang.bigdata.zookeeper;

import com.zhang.bigdata.utils.Constants;
import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ServiceRegistry {
    private CountDownLatch latch = new CountDownLatch(1);
    private ZooKeeper zooKeeper;

    public ServiceRegistry() {
        try {
            zooKeeper = connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ZooKeeper connect() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(Constants.ZK_SERVER_CONNECT_URL, Constants.ZK_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        latch.await();
        return zooKeeper;
    }

    public void registry(String data) {
        try {
            if(zooKeeper.exists(Constants.ZK_SERVER_NODE_PATH, false) == null) {
                zooKeeper.create(Constants.ZK_SERVER_NODE_PATH,"RPC-SERVICE".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String path = Constants.ZK_SERVER_NODE_PATH + "/service";
            zooKeeper.create(path,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
