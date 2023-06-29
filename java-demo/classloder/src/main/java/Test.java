import app.chail.agent.RouterClassLoader;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * s
 *
 * @author liuhuiming
 * @date 2021/09/24 17:29
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = String.class.getClassLoader();
        URLClassLoader appClassLoader =(URLClassLoader) Test.class.getClassLoader();
        List<URL> urlsList = new ArrayList<URL>();
        URL[]  urls = new URL[urlsList.size()];
        urls = urlsList.toArray(urls);
        RouterClassLoader routerClassLoader = new RouterClassLoader(urls, appClassLoader.getParent());
        Field field = ClassLoader.class.getDeclaredField("parent");
        field.setAccessible(true);
        field.set(appClassLoader,routerClassLoader);


        System.out.println("22222222222");
    }
}
