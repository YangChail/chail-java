package com.chail.nio;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class NIOClient {

    public static void main(String[] args) throws IOException {

        System.out.println(fib(3));
//        Socket socket = new Socket("127.0.0.1", 8888);
//        OutputStream out = socket.getOutputStream();
//        String s = "hello world";
//        out.write(s.getBytes());
//        out.close();
    }


    public static int fib(int n) {
        int tmp[] =new int[n+1];
        tmp[0]=0;
        tmp[1]=1;
        for(int i=2;i<=n;i++){
            tmp[n]=tmp[n-1]+tmp[n-2];
            tmp[n]=tmp[n]%1000000007;
        }

        if(tmp[n]==1000000008){
            return 1;
        }else{
            return tmp[n];
        }
    }
}
