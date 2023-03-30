package com.chail.demo;

import java.util.Random;

/**
 * @author : yangc
 * @date :2023/3/22 17:19
 * @description :
 * @modyified By:
 */
public class SleepUtils {

    public static int sleep(){
        Random random = new Random();
        int i = random.nextInt(10);
        try {
            Thread.sleep((i+1)*1000 );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return i;
    }
}
