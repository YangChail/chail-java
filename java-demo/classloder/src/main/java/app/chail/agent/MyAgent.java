package app.chail.agent;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class MyAgent {


    public static void premain(String agentArgs, Instrumentation inst) {
        URLClassLoader appClassLoader =(URLClassLoader) MyAgent.class.getClassLoader();
        List<URL> urlsList = new ArrayList<URL>();
        URL[]  urls = new URL[urlsList.size()];
        urls = urlsList.toArray(urls);
        RouterClassLoader routerClassLoader = new RouterClassLoader(urls, appClassLoader.getParent());
        Field field = null;
        try {
            field = ClassLoader.class.getDeclaredField("parent");
            field.setAccessible(true);
            field.set(appClassLoader,routerClassLoader);
            //保留parent
            Object parent = field.get(appClassLoader);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static void premain(String agentArgs) {


    }



}
