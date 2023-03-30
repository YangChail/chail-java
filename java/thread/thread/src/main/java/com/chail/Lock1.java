package com.chail;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : yangc
 * @date :2023/3/22 10:52
 * @description :
 * @modyified By:
 */
public class Lock1 {

    private static int j = 0;

    public static void main(String[] agrs) throws InterruptedException {
        ReentrantLock nonReentrantLock = new ReentrantLock();

        Runnable runnable = () -> {
            //获取锁
            nonReentrantLock.lock();
            for (int i = 0; i < 100000; i++) {
                j++;
            }
            //释放锁
            nonReentrantLock.unlock();
        };

        Thread thread = new Thread(runnable);
        Thread threadTwo = new Thread(runnable);

        thread.start();
        threadTwo.start();

        thread.join();
        threadTwo.join();

        System.out.println(j);
    }
}
