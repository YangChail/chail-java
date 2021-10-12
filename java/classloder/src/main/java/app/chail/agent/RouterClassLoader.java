package app.chail.agent;

import sun.misc.VM;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class RouterClassLoader extends URLClassLoader {

    private Set<String> classpathSet = new HashSet<>();

    public RouterClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }


    /**
     * 判断这个class是不是输入统一数据源加载
     *
     * @param name
     * @return
     * @throws ClassNotFoundException
     */
    public  Class<?> loadClass(String name)
            throws ClassNotFoundException {





        return super.loadClass(name);
    }



    private boolean check(){
        return false;

    }







}
