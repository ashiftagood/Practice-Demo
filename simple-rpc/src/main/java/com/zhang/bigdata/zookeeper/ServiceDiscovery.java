package com.zhang.bigdata.zookeeper;

import com.zhang.bigdata.utils.Constants;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ServiceDiscovery {
    private CountDownLatch latch = new CountDownLatch(1);
    private static List<String> serviceAddrList;

    public void start() throws Exception {
        ZooKeeper zooKeeper = connect();
        watchZNode(zooKeeper);
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

    private void watchZNode(ZooKeeper zooKeeper) throws Exception {
        List<String> children = zooKeeper.getChildren(Constants.ZK_SERVER_NODE_PATH, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        watchZNode(zooKeeper);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        List<String> serverInfoList = new ArrayList<>();
        for(String s : children) {
            byte[] data = zooKeeper.getData(Constants.ZK_SERVER_NODE_PATH + "/" + s, false, null);
            serverInfoList.add(new String(data));
        }
        serviceAddrList = serverInfoList;
    }

    public static String[] getService() {
        String hostPort;
        if(serviceAddrList.size() == 1) {
            hostPort = serviceAddrList.get(0);
        } else {
            hostPort = serviceAddrList.get(new Random().nextInt(serviceAddrList.size()));
        }
        String[] split = hostPort.split(":");
        return split;
    }
}
