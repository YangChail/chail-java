/**
 * 版权所有：美创科技
 * 项目名称:license
 */
package com.mchz.template.license;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import java.io.*;
import java.security.Key;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.ArrayList;
import java.util.List;

/**
 * @author
 *
 */
public class ReadLicense {

	private static int DECODE = 0;

	private static int ENCODE = 1;

	private static final Logger LOGGER = LoggerFactory.getLogger(ReadLicense.class);
	/**
	 * 对称加密解密 des
	 *
	 * @param order
	 *            加密解密 密钥
	 * @param type
	 *            加密/解密
	 * @param src
	 *            待加密/解密字符串
	 * @return
	 * @throws Exception
	 */
	public byte[] fileDes(String order, Integer type, String src) throws Exception {
		Key key;
		// 初始化DES算法的密钥对象
		KeyGenerator generator = KeyGenerator.getInstance("DES");
		// 防止linux下 随机生成key
		SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
		secureRandom.setSeed(order.getBytes());
		generator.init(56, secureRandom);
		key = generator.generateKey();

		// 创建Cipher对象，ECB模式的DES算法。
		Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
		// 判断是加密还是解密。设置cipher对象为加密或解密。
		if (type == ENCODE) {
			cipher.init(Cipher.ENCRYPT_MODE, key);
			return cipher.doFinal(src.getBytes());
		} else {
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(parseHexStr2Byte(src));
		}
	}

	/**
	 * 返回在文件中指定位置的对象
	 *
	 * @param file
	 *            指定的文件
	 * @param i
	 *            从1开始
	 * @return
	 */
	private Object getObjFromFile(String file, int i) {
		ObjectInputStream ois = null;
		Object obj = null;
		FileInputStream fis = null;
		try {
		    fis = new FileInputStream(file);
			ois = new ObjectInputStream(fis);
			for (int j = 0; j < i; j++) {
				obj = ois.readObject();
			}
		} catch (Exception e) {
			LOGGER.error("aerror", e);
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
			} catch (IOException f) {
				LOGGER.error("fiserror", f);
			}
			try {
				if (ois != null) {
					ois.close();
				}
			} catch (IOException e) {
				LOGGER.error("berror", e);
			}
		}
		return obj;
	}

	/**
	 * 读取整个license
	 *
	 * @param file
	 *            指定的文件
	 * @return
	 */
	private List<Object> getAllObjFromFile(String file) {
		ObjectInputStream ois = null;
		FileInputStream fis = null;
		List<Object> list = new ArrayList<>();
		boolean read = true;
		try {
			fis = new FileInputStream(file);
			ois = new ObjectInputStream(fis);
			while (read) {
				list.add(ois.readObject());
			}
		} catch (EOFException ex) { // 直到读完为止
			read = false;
		} catch (Exception e) {
			LOGGER.error("cerror", e);
		} finally {
			try {
				if (fis != null){
					fis.close();
				}
			} catch (IOException b) {
				LOGGER.error("fiserror", b);
			}
			try {
				if (ois != null){
					ois.close();
				}
			} catch (IOException e) {
				LOGGER.error("derror", e);
			}
		}
		return list;
	}

	/**
	 * 将二进制转化为16进制字符串
	 *
	 * @param b
	 *            二进制字节数组
	 * @return String
	 */
	private String byte2hex(byte[] b) {
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		}
		return hs.toUpperCase();
	}

	/**
	 * 十六进制字符串转化为2进制
	 *
	 * @param hexStr
	 * @return byte[]
	 */
	public static byte[] parseHexStr2Byte(String hexStr) {
		if (hexStr.length() < 1) {
			return new byte[0];
		}
		byte[] result = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length() / 2; i++) {
			int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
			int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
			result[i] = (byte) (high * 16 + low);
		}
		return result;
	}

	/**
	 * 根据公匙，签名，信息验证信息的合法性（读取数字签名文件 时用） (capaa-web使用)
	 *
	 * @param signed
	 *            完整信息
	 * @param info
	 *            实际信息
	 * @param keyUrl
	 *            公钥路径
	 * @return true 验证成功 false 验证失败
	 */
	private boolean validateSign(byte[] signed, String info, String keyUrl) {
		// 读取公匙
		PublicKey mypubkey = (PublicKey) getObjFromFile(keyUrl, 1);

		try {
			// 初始一个Signature对象,并用公钥和签名进行验证
			Signature signetcheck = Signature.getInstance("MD5WithRSA"); // DSA
			// 初始化验证签名的公钥
			signetcheck.initVerify(mypubkey);
			// 使用指定的 byte 数组更新要签名或验证的数据
			signetcheck.update(info.getBytes());
			// 验证传入的签名
			return signetcheck.verify(signed);
		} catch (Exception e) {
			LOGGER.error("eerror", e);
			return false;
		}
	}

	/**
	 * 签名，信息验证信息的合法性（数据库信息组装后用） (capaa-web使用)
	 *
	 * @param signed
	 *            完整信息
	 * @param info
	 *            实际信息
	 * @param keyUrl
	 *            公钥路径
	 * @param order
	 *            加密命令
	 * @return true 验证成功 false 验证失败
	 */
	public boolean validateSign(String signed, String info, String keyUrl, String order) {
		// 读取公匙
		PublicKey mypubkey = (PublicKey) getObjFromFile(keyUrl, 1);

		byte[] encode;
		try {
			encode = fileDes(order, ENCODE, info);
		} catch (Exception e1) {
			LOGGER.error("ferror", e1);
			return false;
		}
		try {
			// 初始一个Signature对象,并用公钥和签名进行验证
			Signature signetcheck = Signature.getInstance("MD5WithRSA"); // DSA
			// 初始化验证签名的公钥
			signetcheck.initVerify(mypubkey);
			// 使用指定的 byte 数组更新要签名或验证的数据
			signetcheck.update(byte2hex(encode).getBytes());
			// 验证传入的签名
			return signetcheck.verify(parseHexStr2Byte(signed));
		} catch (Exception e) {
			LOGGER.error("gerror", e);
			return false;
		}
	}

	/**
	 * 取license内容
	 *
	 * @param fileurl
	 *            license文件路径
	 * @param order
	 *            （加密命令要与解密命令一致）
	 * @param keyUrl
	 *            公钥路径
	 * @return license信息
	 */
	public List<String> getLicenseInfo(String fileurl, String order, String keyUrl) throws Exception {
		List<Object> ls = getAllObjFromFile(fileurl); // 将license全部读出
		List<String> list = new ArrayList<>();
		for (int i = 0; i < ls.size(); i = i + 2) {
			String signed = (String) ls.get(i);
			String info = (String) ls.get(i + 1);
			// 利用公匙对签名进行验证。
			if (validateSign(parseHexStr2Byte(signed), info, keyUrl)) {

				try {
					byte[] decode = fileDes(order, DECODE, info);
					list.add(new String(decode) + "|" + signed);
				} catch (Exception e) {
					throw new Exception("herror");
				}

			} else {// 完整性验证出错
				throw new Exception("tsettest2:" + signed);
			}

		}

		return list;

	}

	/**
	 * 测试
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		ReadLicense jiami = new ReadLicense();

		String path = System.getProperties().getProperty("user.dir") + File.separator + "license"
				+ File.separator + "public.key";
		String apath = System.getProperties().getProperty("user.dir") + File.separator
				+ "license" + File.separator + "2022-01-03.license";

		// 返回licence的信息
		List<String> infos = jiami.getLicenseInfo(
				apath, "capaa", path);



		for (String info : infos) {
			System.out.println(info);
		}
		infos = jiami.getLicenseInfo(apath, "capaa", path);

		for (String info : infos) {
			System.out.println(info);
		}
	}
}
