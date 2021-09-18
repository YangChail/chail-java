package com.chail.apputil.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * 反射获取变量
 * @author yangc
 *
 */
public class AppTest {

    public static String  hehe = "aa";

    public String xixi = "xixi";

    public static void test() {
        Field[] fields = AppTest.class.getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                if(field.getType().toString().endsWith("java.lang.String") && Modifier.isStatic(field.getModifiers()))
                    System.out.println(field.getName() + " , " + field.get(AppTest.class));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
    	test();
	}

}