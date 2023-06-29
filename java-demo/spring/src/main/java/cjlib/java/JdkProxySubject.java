package cjlib.java;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class JdkProxySubject implements InvocationHandler {


    //引入要代理的真实对象
    private RealSubject realSubject;

    //用构造器注入目标方法，给我们要代理的真实对象赋初值
    public JdkProxySubject(RealSubject realSubject) {
        this.realSubject = realSubject;
    }

    //实现接口的方法
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("before");
        Object result = null;
        try {
            //调用目标方法
            //利用反射构造目标对象
            //    当代理对象调用真实对象的方法时，其会自动的跳转到代理对象关联的handler对象的invoke方法来进行调用
            result = method.invoke(realSubject, args);

        } catch (Exception e) {
            System.out.println("ex:" + e.getMessage());
            throw e;
        } finally {
            System.out.println("after");
        }
        return result;
    }
}