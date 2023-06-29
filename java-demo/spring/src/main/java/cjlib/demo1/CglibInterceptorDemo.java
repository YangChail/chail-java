package cjlib.demo1;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * 1. CGLIB包的底层是通过使用一个小而快的字节码处理框架ASM，来转换字节码并生成新的类
 * 2.  3. JDK的动态代理有一个限制，就是使用动态代理的对象必须实现一个或多个接口。
 * 4. 如果想代理没有实现接口的类，就可以使用CGLIB实现
 */
public class CglibInterceptorDemo implements MethodInterceptor {

    private Object target;//目标代理对象

    public CglibInterceptorDemo(Object target) {
        this.target = target;
    }

    //为目标对象生成代理对象
    public Object getProxyInstance() {

        //工具类
        Enhancer enhancer = new Enhancer();
        //设置父类
        enhancer.setSuperclass(target.getClass());

        //设置回调函数
        enhancer.setCallback(this);

        //创建子类对象代理
        return enhancer.create();
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        System.out.println("开启事务");
        //执行目标对象的方法
        Object result = method.invoke(target, objects);
        System.out.println("提交事物");
        System.out.println("方法的返回值：" + result);
        return result;
    }
}