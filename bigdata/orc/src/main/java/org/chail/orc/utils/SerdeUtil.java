package org.chail.orc.utils;

/**
 * Description:
 * datetime: 2020/5/18 14:04
 * author: ningyu
 */
public class SerdeUtil {

    public static boolean isLazyBinaryColumnarSerDe(String serde){
        return "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe".equals(serde);
    }

}
