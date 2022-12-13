package chail;

/**
 * @author : yangc
 * @date :2022/5/31 17:33
 * @description :
 * @modyified By:
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public  @interface Db2ObjectFiled {


    /**
     * datasupport 本地key
     *
     * @return
     */
    String value() ;

}
