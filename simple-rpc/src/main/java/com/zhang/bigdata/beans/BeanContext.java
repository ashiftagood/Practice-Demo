package com.zhang.bigdata.beans;

import com.zhang.bigdata.annotation.RpcService;
import com.zhang.bigdata.utils.ProxyUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeanContext {
    private Map<String, Object> beansMap;
    private volatile static BeanContext beanContext;

    private BeanContext() {
        beansMap = new HashMap<>();
    }

    public static BeanContext getInstance() {
        if(beanContext == null) {
            beanContext = new BeanContext();
        }
        return beanContext;
    }

    public List<Class> initBeanContext(List<String> classNameList, Class annotationClass) throws Exception{
        List<Class> classList = new ArrayList<>();
        for(String className : classNameList) {
            Class<?> aClass = Class.forName(className);
            RpcService rpcService = (RpcService) aClass.getAnnotation(annotationClass);
            if(rpcService != null) {
                Object o = aClass.newInstance();
                beanContext.beansMap.put(rpcService.value().getName(), o);
            } else {
                classList.add(aClass);
            }
        }
        return classList;
    }

    public void autoInjectProxyObject(List<Class> classList, Class annotationClass) throws Exception {
        for(Class clazz : classList) {
            Field[] fields = clazz.getDeclaredFields();
            for(Field field : fields) {
                field.setAccessible(true);
                Annotation annotation = field.getAnnotation(annotationClass);
                if(annotation != null) {
                    Class<?> type = field.getType();
                    Object object = ProxyUtil.createObject(type);
                    field.set(clazz.newInstance(),object);
                }
            }
        }
    }

    public Object getBean(String beanName) {
        return beanContext.beansMap.get(beanName);
    }

    public int getBeansNum() {
        return beanContext.beansMap.size();
    }
}
