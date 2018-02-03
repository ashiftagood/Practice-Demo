package com.zhang.bigdata.annotation;

import com.zhang.bigdata.beans.BeanContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 根据文件名扫描注解
 */
public class AnnotationScanner {

    private String scanPackage;
    private BeanContext beanContext;

    public AnnotationScanner(String scanPackage, BeanContext beanContext) {
        this.scanPackage = scanPackage;
        this.beanContext = beanContext;
    }

    public void scan() {
        if(beanContext.getBeansNum() == 0) {
            String basePath = AnnotationScanner.class.getResource("/").getPath();
            String scanPath = basePath + scanPackage.replaceAll("\\.","/");
            File scanDir = new File(scanPath);
            List<String> classNameList = getClassName(scanDir);
            try {
                List<Class> classList = beanContext.initBeanContext(classNameList, RpcService.class);
                beanContext.autoInjectProxyObject(classList, RpcInject.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> getClassName(File scanFile) {
        List<String> classNameList = new ArrayList<>();
        if(scanFile.isDirectory()) {
            File[] files = scanFile.listFiles();
            for(File file : files) {
                if(file.isDirectory()) {
                    getClassName(file);
                } else {
                    String className = scanPackage + "." + file.getName().split("\\.")[0];
                    classNameList.add(className);
                }
            }
        }
        return classNameList;
    }
}
