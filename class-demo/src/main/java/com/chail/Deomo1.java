package com.chail;

public class Deomo1 {

    private static String aa;
    static {
        try {
            aa = Class2.excute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public Deomo1() {
        try {
            aa = Class2.excute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void exc(){
        System.out.println("sadasdasd");
    }



    public static void exc2(){
        System.out.println("sadasdasd");
    }



}
