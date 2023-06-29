package cjlib.java;

public class Client {
    public static void main(String[] args) {
        //通过接口
        Subject subject = new Proxy(new RealSubject());
        subject.test();


        //使用Proxy构造对象
        //参数
        //java泛型需要转换一下
        // 通过Proxy的newProxyInstance方法来创建我们的代理对象，我们来看看其三个参数
        //     * 第一个参数 getClassLoader() ，我们这里使用Client这个类的ClassLoader对象来加载我们
        //        的代理对象
        //     * 第二个参数表示我要代理的是该真实对象，这样我就能调用这组接口中的方法了
        //     * 第三个参数handler，我们这里将这个代理对象关联到了上方的 InvocationHandler这个对象上

        Subject subject1 =
                (Subject) java.lang.reflect.Proxy.newProxyInstance
                        (Client.class.getClassLoader(),
                        new Class[]{Subject.class}
                                , new JdkProxySubject(new RealSubject()));
        //调用方法
        subject1.test();


    }
}