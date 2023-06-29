package cjlib.demo2;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * 中介代理类
 * @author jkl
 *
 */
public class CglibMediaInterceptor implements MethodInterceptor{

    //需要被代理的对象
    private Object target;

    /**
     * 设置代理类对象
     * @param target
     */
    public void setTarget(Object target) {
        this.target = target;
    }

    public Object getTarget() {
        return target;
    }

    public Object getProxy(){
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(this.getTarget().getClass());
        Object obj =enhancer.create();
        enhancer.setCallback(this);
        return obj;
    }

    /**
     * 通过 method 引用实例    Object result = method.invoke(target, args); 形式反射调用被代理类方法，
     * target 实例代表被代理类对象引用, 初始化 CglibMethodInterceptor 时候被赋值 。但是Cglib不推荐使用这种方式
     * @param obj    代表Cglib 生成的动态代理类 对象本身
     * @param method 代理类中被拦截的接口方法 Method 实例
     * @param args   接口方法参数
     * @param proxy  用于调用父类真正的业务类方法。可以直接调用被代理类接口方法
     * @return
     * @throws Throwable
     */
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        Object result = null;
        try {
            media();//中介自己的相关业务
            result = proxy.invokeSuper(obj, args);//房东业务: 处理收租和退租
            userLogs(method.getName());//记录操作日志
            System.out.println("==============================="+proxy.getSuperName());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void media(){

        System.out.println("与中介交谈租房相关事项");
    }

    public void userLogs(String method){

        System.out.println("记录用户操作日志，执行了:"+method+"方法");
    }
}

