package com.chail.apputil.file;

import java.util.HashMap;
import java.util.List;


public class ReadLog {
	
	public static HashMap<String,Integer> IPMAP=new HashMap<>();
	public static void main(String[] args) {
		List<String> readFileByLines = ReadFromFile.readFileByLines("D:/aaa.log");
		for(String str:readFileByLines) {
			if(str.indexOf("Chrome/84.0.4147")>-1||str.indexOf("Linux; Android 10; MI 9")>-1) {
				continue;
			}
			String[] split = str.split(" ");
			int i=split.length;
			
			if(str.indexOf("Android 9")>-1&&str.indexOf("PPR1.180610.011")>-1) {
				//System.out.println(str);
				//print(split);
				continue;
			}
			if(str.indexOf("Chrome/81.0.4044.122")>-1) {
				//System.out.println(str);
				//print(split);
				continue;
			}
			printError(split, str);
		}
		
	}
	
	
	private static void print(String []split) {
		System.out.println("*************************");
		System.out.println("时间:  "+split[3]+"   IP: "+split[0]);
		System.out.println("访问连接："+split[10]);
		System.out.println("*************************");
	}
	
	
	private static void printError(String []split,String str) {
		Integer addCode = addCode(split[3]);
		if(addCode<3) {
			return;
		}
		if(addCode>3) {
			System.out.println("ip次数:  "+addCode);
			System.out.println("时间:  "+split[3]+"   IP: "+split[0]);
			System.out.println(str);
		}
		
		
		System.out.println("---------------------");
	}
	
	
	private static Integer  addCode(String ip) {
		Integer integer =1;
		if(IPMAP.containsKey(ip)) {
			integer = IPMAP.get(ip);
			integer=integer+1;
		}
		IPMAP.put(ip, integer);
		return integer;
	}
	
}
