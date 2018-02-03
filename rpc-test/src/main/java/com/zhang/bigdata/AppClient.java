package com.zhang.bigdata;

import com.zhang.bigdata.annotation.RpcInject;
import com.zhang.bigdata.server.RpcServer;

public class AppClient {

    @RpcInject
    private static HelloWorld helloWorld;

    public static void main(String[] args) throws Exception {
        RpcServer rpcServer = new RpcServer();
        rpcServer.init("com.zhang.bigdata");

        System.out.println(helloWorld.sayHi());
        Person person = new Person();
        person.setName("ZhangCun");
        person.setAge(22);
        System.out.println(helloWorld.personSayHi(person.toString()));

        System.out.println(helloWorld.getPerson());

        System.out.println(helloWorld.getAge("YaoJia"));
    }
}
