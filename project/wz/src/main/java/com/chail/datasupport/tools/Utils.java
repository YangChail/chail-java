package com.chail.datasupport.tools;

/**
 * @author : yangc
 * @date :2022/6/12 19:04
 * @description :
 * @modyified By:
 */
public class Utils {



    public static void delLog(){

        String code="delete  from xxl_job_log where trigger_time  < to_date('2022-06-11','YYYY-MM-DD')\n";
    }
}
