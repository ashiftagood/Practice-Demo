package com.zhang.bigdata.server;

import com.alibaba.fastjson.JSON;
import com.zhang.bigdata.annotation.AnnotationScanner;
import com.zhang.bigdata.beans.BeanContext;
import com.zhang.bigdata.utils.Request;
import com.zhang.bigdata.utils.Response;
import com.zhang.bigdata.zookeeper.ServiceDiscovery;
import com.zhang.bigdata.zookeeper.ServiceRegistry;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class RpcServer {
    private String serverHost;
    private int serverPort;
    private boolean initServer;

    public RpcServer() {
        initServer = false;
    }

    public RpcServer(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.initServer = true;
    }

    public void init(String scanPackage) throws Exception {
        BeanContext beanContext = BeanContext.getInstance();
        AnnotationScanner annotationScanner = new AnnotationScanner(scanPackage, beanContext);
        annotationScanner.scan();
        new ServiceDiscovery().start();
        if(initServer) {
            initServer();
        }
    }

    private void initServer() {
        ServiceRegistry serviceRegistry = new ServiceRegistry();
        serviceRegistry.registry(serverHost + ":" + serverPort);
        startNetServer();
    }

    private void startNetServer() {
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(serverPort),1024);
            serverSocketChannel.configureBlocking(false);
            Selector selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while(true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while(iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();

                    if(selectionKey.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) selectionKey.channel();
                        SocketChannel socketChannel = server.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector, SelectionKey.OP_READ);
                    }
                    if(selectionKey.isReadable()) {
                        SocketChannel readChannel = (SocketChannel) selectionKey.channel();
                        ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
                        readChannel.read(byteBuffer);
                        byteBuffer.flip();
                        byte[] bytes = new byte[byteBuffer.remaining()];
                        byteBuffer.get(bytes);
                        bytes = methodInvoke(bytes);
                        ByteBuffer writeBuffer = ByteBuffer.allocate(bytes.length);
                        writeBuffer.put(bytes);
                        writeBuffer.flip();
                        readChannel.write(writeBuffer);
                        System.out.println("send response : " + new String(bytes));
                        selectionKey.cancel();
                        selectionKey.channel().close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] methodInvoke(byte[] bytes) {
        System.out.println("receive request : " + new String(bytes));
        Request request = JSON.parseObject(bytes, Request.class);
        Response response = new Response();
        try {
            BeanContext beanContext = BeanContext.getInstance();
            Object bean = beanContext.getBean(request.getClassName());
            Class<?> aClass = bean.getClass();
            Method method = aClass.getMethod(request.getMethodName(), request.getParameterTypes());

            Object invoke = method.invoke(bean, request.getParameters());
            response.setObject(invoke);
            response.setErrMsg("");
            return JSON.toJSONBytes(response);
        } catch (Exception e) {
            e.printStackTrace();
            response.setErrMsg("something wrong");
            return JSON.toJSONBytes(response);
        }
    }
}
