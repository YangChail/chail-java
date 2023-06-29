package cjlib.demo1;

/**
 1. cglib代理无需实现接口，通过生成类字节码实现代理，比反射稍快，不存在性能问题，
 2. 但cglib会继承目标对象，需要重写方法，所以目标对象不能为final类
 3.  */
public class CglibTest {

    public static void main(String[] args) {

        //被代理对象
        UserDaoImpl userDao = new UserDaoImpl();
        System.out.println(userDao.getClass());
        userDao.add(33,5);
        //获取代理对象
        CglibInterceptorDemo methodInterceptorDemo = new CglibInterceptorDemo(userDao);
        UserDaoImpl proxy = (UserDaoImpl) methodInterceptorDemo.getProxyInstance();
        System.out.println(proxy.getClass());
        proxy.add(33, 5);
    }
}