package cjlib.demo3;

import cjlib.demo2.CglibMediaInterceptor;
import cjlib.demo2.LandlordSerivce;
import cjlib.demo2.LandlordSerivceImpl;
import net.sf.cglib.core.DebuggingClassWriter;

import java.lang.reflect.Field;
import java.util.Properties;

/**
 * 租客客户端
 * @author jkl
 *
 */
public class TenantsClient2 {
    public static void main(String[] args) throws Exception {
        String property = System.getProperty("user.dir");
        String path = "D:/test";
        String userDir = "user.dir";
//		saveGeneratedCGlibProxyFiles(System.getProperty(path));
        System.setProperty(DebuggingClassWriter.DEBUG_LOCATION_PROPERTY, path);
        CglibMediaInterceptor cglib = new CglibMediaInterceptor();
        LandlordSerivce serivce = new LandlordSerivceImpl();
        cglib.setTarget(serivce);
        LandlordSerivce landlordSerivce =(LandlordSerivce)cglib.getProxy();
        landlordSerivce.rent();
        landlordSerivce.without();

    }

    /**
     * 设置保存Cglib代理生成的类文件。
     */
    public static void saveGeneratedCGlibProxyFiles(String dir) throws Exception {
        Field field = System.class.getDeclaredField("props");
        field.setAccessible(true);
        Properties props = (Properties) field.get(null);
        System.setProperty(DebuggingClassWriter.DEBUG_LOCATION_PROPERTY, dir);//dir为保存文件路径
        props.put("net.sf.cglib.core.DebuggingClassWriter.traceEnabled", "true");
    }

}
