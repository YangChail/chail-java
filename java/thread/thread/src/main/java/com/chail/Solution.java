package com.chail;

public class Solution {


    public static void main(String[] args) throws InterruptedException {
        Thread[] threads = new Thread[10];
        for(int i = 0;i<10;i++){
            threads[i] = new Thread(new Plus());
            threads[i].start();
        }
        for(int i = 0;i<10;i++){
            threads[i].join();
        }
        System.out.println(Plus.count);
    }

}

class Plus implements Runnable {
   public static int count = 0;

    public static synchronized   void add() {
        count++;
    }

    @Override
    public void run() {
        for (int k = 0; k < 10000; k++) {
            add();

        }
    }
}