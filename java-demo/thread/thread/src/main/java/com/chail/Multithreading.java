package com.chail;

import org.openjdk.jol.info.ClassLayout;

/**
 * @author : yangc
 * @date :2023/3/20 16:40
 * @description : 线程可见性问题
 *  java jmm 问题 java内存模型
 *
 *
 * @modyified By:
 */
public class Multithreading {
    public  static volatile int a=1;
    public static void main(String[] args) throws InterruptedException {
        Integer b=new Integer(10);
        // 睡眠 5s
        Thread.sleep(5000);
        Object o = new Object();
        System.out.println("未进入同步块，MarkWord 为：");
        System.out.println(ClassLayout.parseInstance(o).toPrintable());
        synchronized (o){
            System.out.println(("进入同步块，MarkWord 为："));
            System.out.println(ClassLayout.parseInstance(o).toPrintable());
        }
        Thread t2 = new Thread(() -> {
            synchronized (o) {
                System.out.println("新线程获取锁，MarkWord为：");
                System.out.println(ClassLayout.parseInstance(o).toPrintable());
            }
        });
        t2.start();
        t2.join();
        System.out.println("主线程再次查看锁对象，MarkWord为：");
        System.out.println(ClassLayout.parseInstance(o).toPrintable());
        synchronized (o){
            System.out.println(("主线程再次进入同步块，MarkWord 为："));
            System.out.println(ClassLayout.parseInstance(o).toPrintable());
        }

        synchronized (b) {
            System.out.println(("主线程再次进入同步块，并且调用hashcode方法，MarkWord 为："));
            b.hashCode();
            System.out.println(ClassLayout.parseInstance(b).toPrintable());
        }

    }



}
