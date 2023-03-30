package com.chail;

import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : yangc
 * @date :2023/3/21 9:49
 * @description :
 * @modyified By:
 */
public class MyLock {


    public static void main(String[] args) throws InterruptedException {

        Object o = new Object();
        System.out.println("还没有进入到同步块");
        System.out.println("markword:" + ClassLayout.parseInstance(o).toPrintable());
        //默认JVM启动会有一个预热阶段，所以默认不会开启偏向锁
        Thread.sleep(5000);
        Object b = new Object();
        System.out.println("还没有进入到同步块");
        System.out.println("markword:" + ClassLayout.parseInstance(b).toPrintable());
        synchronized (o){
            System.out.println("进入到了同步块");
            System.out.println("markword:" + ClassLayout.parseInstance(o).toPrintable());
        }
    }
}
