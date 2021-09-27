import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class RouterClassLoader extends URLClassLoader {

    private Set<String> classpathSet=new HashSet<>();
    public RouterClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }


    /**
     * 判断这个class是不是输入统一数据源加载
     *
     * @param name
     * @param resolve
     * @return
     * @throws ClassNotFoundException
     */
        public final Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {




        return super.loadClass(name, resolve);
    }













}
