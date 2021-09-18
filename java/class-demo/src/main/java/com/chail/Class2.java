package com.chail;

public class Class2 {

    public static String excute() throws Exception{
        System.out.println("读取类");
        if(false){
            return "aaaa";
        }
        throw new Exception("抛异常");
    }


}
