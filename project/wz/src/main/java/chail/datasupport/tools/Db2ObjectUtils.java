package chail.datasupport.tools;

import chail.Db2ObjectFiled;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * @author : yangc
 * @date :2022/5/31 17:38
 * @description :
 * @modyified By:
 */
public class Db2ObjectUtils {


    public static void getObj(ResultSet resultSet, Object object, Class<?> objClass) throws IllegalAccessException, SQLException, IntrospectionException, InvocationTargetException {
        Field[] declaredFields = objClass.getDeclaredFields();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Integer> col = new HashMap<>();
        if (resultSet.getRow() < 1) {
            return;
        }
        for (int i = 1; i <= columnCount; i++) {
            col.put(metaData.getColumnName(i), i);
        }
        for (Field field : declaredFields) {
            Db2ObjectFiled annotation = field.getAnnotation(Db2ObjectFiled.class);
            if (annotation == null) {
                continue;
            }
            PropertyDescriptor pd = new PropertyDescriptor(field.getName(), objClass);
            Method method = pd.getWriteMethod();
            method.setAccessible(true);
            String colKey = annotation.value();
            if (col.containsKey(colKey)) {
                Integer index = col.get(colKey);
                String value = resultSet.getObject(index).toString();
                method.invoke(object, value);
            }
        }
    }
}
