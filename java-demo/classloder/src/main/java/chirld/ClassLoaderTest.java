package chirld;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;

public class ClassLoaderTest {
    static class MyClassLoader extends ClassLoader {
        public Class<?> getClass(byte[] byteArray, int off, int len) {
            return super.defineClass(byteArray, off, len);
        }
    }

    public static void main(String[] args) throws Exception {
        MyClassLoader myClassLoader = new MyClassLoader();
        File classFile = new File(
                "D:\\code\\chail-java\\java-demo\\classloder\\target\\classes\\chirld\\Dog.class");
        ByteArrayOutputStream baos = null;
        BufferedOutputStream bos = null;
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        fis = new FileInputStream(classFile);
        baos = new ByteArrayOutputStream();
        bis = new BufferedInputStream(fis);
        bos = new BufferedOutputStream(baos);
        byte[] buff = new byte[1024];
        int len = -1;
        while ((len = bis.read(buff, 0, 1024)) != -1) {
            bos.write(buff, 0, len);
        }
        bos.flush();
        byte[] byteArray = baos.toByteArray();


        Class<Dog> clazz1 = (Class<Dog>) myClassLoader.getClass(byteArray, 0,
                byteArray.length);
        Object dog1 = clazz1.newInstance();
        clazz1.getDeclaredField("name").set(dog1, "张三");


        Class clazz2 = Class.forName("chirld.Dog");
        Object dog2 = clazz2.newInstance();
        clazz2.getDeclaredField("name").set(dog1, "李四");

        Dog dog = new Dog();
        dog.name="王五";

        System.out.println(clazz1.getDeclaredField("name").get(dog1));
        System.out.println(clazz2.getDeclaredField("name").get(dog2));

        System.out.println(dog.name);
        System.out.println(clazz1.getDeclaredField("name").get(dog1));

        Class clazz3 = Dog.class;
        System.out.println("clazz1类装载器为" + clazz1.getClassLoader()
                + ",clazz2类装载器为" + clazz2.getClassLoader() + ",clazz3类装载器为"
                + clazz3.getClassLoader());
        System.out.println("clazz1 == clazz2 ?" + (clazz1 == clazz2));
        System.out.println("clazz2 == clazz3 ?" + (clazz2 == clazz3));
        System.out.println("clazz1 == clazz3 ?" + (clazz1 == clazz3));


    }
}
