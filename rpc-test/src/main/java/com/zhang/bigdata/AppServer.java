package com.zhang.bigdata;

import com.zhang.bigdata.server.RpcServer;

public class AppServer {
    public static void main(String[] args) throws Exception {
        RpcServer rpcServer = new RpcServer("localhost",9999);
        rpcServer.init("com.zhang.bigdata");
    }
}
