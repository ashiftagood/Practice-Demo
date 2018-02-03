package com.zhang.bigdata.zookeepr;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 利用zookeeper感知服务上下线
 */
public class ServicePerception {
    private static String connectUrl = "192.168.229.129:2181,192.168.229.130:2181,192.168.229.131:2181";
    private static int timeout = 2000;

    public static void main(String[] args) throws Exception {
        Server server01 = new Server(connectUrl, timeout, "server01");
        Server server02 = new Server(connectUrl, timeout, "server02");
        Server server03 = new Server(connectUrl, timeout, "server03");

        Thread t1 = new Thread(server01);
        Thread t2 = new Thread(server02);
        Thread t3 = new Thread(server03);

        Thread.sleep(3000);
        t1.start();
        Thread.sleep(3000);
        t2.start();
        Thread.sleep(3000);
        t3.start();
        Thread.sleep(5000);

        Thread.sleep(5000);
        server01.shutdown();
        Thread.sleep(5000);
        server02.shutdown();
        Thread.sleep(5000);
        server03.shutdown();
    }

}

class Clients {
    private static String connectUrl = "192.168.229.129:2181,192.168.229.130:2181,192.168.229.131:2181";
    private static int timeout = 2000;
    public static void main(String[] args) {
        Client client01 = new Client(connectUrl, timeout, "client01");
        Thread c1 = new Thread(client01);
        c1.start();
    }
}

class Server implements Runnable {

    private ZooKeeper zooKeeper;
    private String serverName;
    private boolean shutdown = false;

    public Server(String connectUrl, int timeout, String serverName) {
        this.serverName = serverName;
        try {
            zooKeeper = new ZooKeeper(connectUrl, timeout, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        shutdown = true;
    }

    public void run() {
        try {
            Stat stat = zooKeeper.exists("/servers",false);
            if(stat == null) {
                zooKeeper.create("/servers","this is parent znode of server".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zooKeeper.create("/servers/server",serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(serverName + " is online");

            while (!shutdown) {
                System.out.println(serverName + " is waiting for job");
                Thread.sleep(2000);
            }
            System.out.println(serverName + " is died");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class Client implements Runnable {
    private String clientName;
    private ZooKeeper zooKeeper;
    private boolean shutdown = false;

    public Client(String connectUrl, int timeout, String clientName) {
        this.clientName = clientName;
        try {
            zooKeeper = new ZooKeeper(connectUrl, timeout, we -> getServerList());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        shutdown = true;
    }

    public void run() {
        try {
            while (!shutdown) {
                System.out.println(clientName + " is working");
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getServerList() {
        try {
             List<String> list = zooKeeper.getChildren("/servers",true);
            System.out.println("online server count:" + list.size());
             for(String s : list) {
                 byte[] bytes = zooKeeper.getData("/servers/"+s, false, null);
                 System.out.println("serverName:" + s + ",data:" + new String(bytes));
             }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
