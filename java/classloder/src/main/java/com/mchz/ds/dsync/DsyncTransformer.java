package com.mchz.ds.dsync;

import com.mchz.ds.dsync.ZkRegister;
import javassist.*;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DsyncTransformer implements ClassFileTransformer {

    // 被处理的方法列表
    final static Map<String, List<String>> methodMap = new HashMap<String, List<String>>();

    public DsyncTransformer() {
        add("com.mchz.dsync.deployer.DSyncStarter.start");
    }

    private void add(String methodString) {
        String className = methodString.substring(0, methodString.lastIndexOf("."));
        String methodName = methodString.substring(methodString.lastIndexOf(".") + 1);
        List<String> list = methodMap.get(className);
        if (list == null) {
            list = new ArrayList<String>();
            methodMap.put(className, list);
        }
        list.add(methodName);
    }

    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        className = className.replace("/", ".");
        try {
            if (methodMap.containsKey(className)) {
                CtClass ctclass = ClassPool.getDefault().get(className);
                for (String methodName : methodMap.get(className)) {
                    CtMethod ctmethod = ctclass.getDeclaredMethod(methodName);
                    ctmethod.insertBefore("com.mchz.ds.dsync.ZkRegister.registry(); \nlogger.info(\"dsync Regist Zk.success\");");
                    ZkRegister.registry();
                }
                return ctclass.toBytecode();
            }

        } catch (NotFoundException | CannotCompileException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private void test(){

    }

}
