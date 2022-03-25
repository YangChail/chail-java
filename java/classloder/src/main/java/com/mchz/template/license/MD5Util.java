/**
 * 版权所有：美创科技
 * 项目名称:b-capaa-web-2.3.0.16.1
 * 创建者: jinmq
 * 创建日期: 2012-11-20
 * 文件说明:
 * 最近修改者：jinmq
 * 最近修改日期：2012-11-20
 */
package com.mchz.template.license;


import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;


/**
 * @author jinmq
 */
public class MD5Util {
	private static final Logger LOGGER = LoggerFactory.getLogger(MD5Util.class);
    private static final char[] md5String = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
	public static String md5(String pwd) {

        // 用于加密的字符
		try {
			// 使用平台的默认字符集将此 String 编码为 byte序列，并将结果存储到一个新的 byte数组中
			byte[] btInput = pwd.getBytes("UTF-8");
			// 获得指定摘要算法的 MessageDigest对象，此处为MD5
			// MessageDigest类为应用程序提供信息摘要算法的功能，如 MD5 或 SHA 算法。
			// 信息摘要是安全的单向哈希函数，它接收任意大小的数据，并输出固定长度的哈希值。
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			// System.out.println(mdInst);
			// MD5 Message Digest from SUN, <initialized>
			// MessageDigest对象通过使用 update方法处理数据， 使用指定的byte数组更新摘要
			mdInst.update(btInput);
			// System.out.println(mdInst);
			// MD5 Message Digest from SUN, <in progress>
			// 摘要更新之后，通过调用digest（）执行哈希计算，获得密文
			byte[] md = mdInst.digest();
			// System.out.println(md);
			// 把密文转换成十六进制的字符串形式
			int j = md.length;
			// System.out.println(j);
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) { // i = 0
				byte byte0 = md[i]; // 95
				str[k++] = md5String[byte0 >>> 4 & 0xf]; // 5
				str[k++] = md5String[byte0 & 0xf]; // F
			}
			// 返回经过加密后的字符串
			String res= new String(str);
			//FIXME debug xinggang 20210623
//            LOGGER.info("md5 {} generated for characters {}",res,pwd);
            return res;
		} catch (Exception e) {
			LOGGER.error("加密出错", e);
			return null;
		}
	}


	public static void main(String[] args) {
		System.out.print("请输入要加密的字符串：");
		BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		try {
			String s=br.readLine();
			System.out.println("MD5： "+md5(s));
			System.out.println("MD51："+ DigestUtils.md5Hex(s).toUpperCase());
		} catch (IOException e) {
			LOGGER.error("加密出错", e);
		}

	}
}
