package com.zhang.bigdata;

import com.zhang.bigdata.annotation.RpcService;

@RpcService(HelloWorld.class)
public class HelloWorldImpl implements HelloWorld{
    @Override
    public String sayHi() {
        return "Hello World!";
    }

    @Override
    public String personSayHi(String person) {
        return "Hello " + person;
    }

    @Override
    public String getPerson() {
        Person person = new Person();
        person.setName("YaoJia");
        person.setAge(25);
        return person.toString();
    }

    @Override
    public int getAge(String name) {
        return name.length();
    }
}
