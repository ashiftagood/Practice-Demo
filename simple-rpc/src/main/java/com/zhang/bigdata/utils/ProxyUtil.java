package com.zhang.bigdata.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhang.bigdata.zookeeper.ServiceDiscovery;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;

public class ProxyUtil {

    public static <T> T createObject(Class<?> interfaceClass) {
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[]{interfaceClass}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Request request = new Request();
                request.setClassName(method.getDeclaringClass().getName());
                request.setMethodName(method.getName());
                request.setParameterTypes(method.getParameterTypes());
                request.setParameters(args);

                byte[] bytes = JSON.toJSONBytes(request);
                byte[] responseBytes = sendRequest(bytes, ServiceDiscovery.getService());
                Response response = JSON.parseObject(responseBytes, Response.class);
                if(response.getErrMsg().equals("")) {
                    return response.getObject();
                } else {
                    return null;
                }
            }
        });
    }

    private static byte[] sendRequest(byte[] requestBytes, String[] hostPort) {
        Socket socket = null;
        OutputStream os = null;
        InputStream is = null;
        try {
            socket = new Socket(hostPort[0], Integer.parseInt(hostPort[1]));
            os = socket.getOutputStream();
            is = socket.getInputStream();

            os.write(requestBytes);
            System.out.println("send request : " + new String(requestBytes));
            os.flush();

            byte[] bytes = new byte[1024];
            while((is.read(bytes)) != -1 );
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
                os.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
