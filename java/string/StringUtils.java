package com.chail.apputil.string;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;


public class StringUtils {

	public static final int	All_UPPERCASE	= 2;
	public static final int	All_LOWERCASE	= 1;
	public static final int	FIRST_UPPERCASE	= 3;
	public static final int	ERROR			= -1;

	// private static final String pointNumReg = "/^\\d+(\\.\\d+)?$/";
	// private static final String symbolReg = "\\p{Punct}+";
	/**
	 * 汉字转拼音
	 * 
	 * @throws BadHanyuPinyinOutputFormatCombination
	 */
	public static String chineseToPinyin(String str) throws BadHanyuPinyinOutputFormatCombination {
		HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
		defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);// 输出拼音全部小写
		defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
		defaultFormat.setVCharType(HanyuPinyinVCharType.WITH_V);
		char[] t1 = str.toCharArray();
		StringBuffer sbf = new StringBuffer();
		for (int i = 0; i < t1.length; i++) {
			String[] t2 = PinyinHelper.toHanyuPinyinStringArray(t1[i], defaultFormat);
			sbf.append(StringUtils.upperFirstCase(t2[0]));
		}
		return sbf.toString();
	}

	public static List<String> chineseToPinyinArray(String str) throws BadHanyuPinyinOutputFormatCombination {
		List<String> arrayList = new ArrayList<String>();
		HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
		defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);// 输出拼音全部小写
		defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
		defaultFormat.setVCharType(HanyuPinyinVCharType.WITH_V);
		char[] t1 = str.toCharArray();
		for (int i = 0; i < t1.length; i++) {
			String[] t2 = PinyinHelper.toHanyuPinyinStringArray(t1[i], defaultFormat);
			arrayList.add(StringUtils.upperFirstCase(t2[0]));
		}
		return arrayList;
	}

	/**
	 * 首字母大写
	 * 
	 * @param str
	 * @return
	 */
	public static String upperFirstCase(String str) {
		char[] ch = str.toCharArray();
		if (ch[0] >= 'a' && ch[0] <= 'z') {
			ch[0] = (char) (ch[0] - 32);
		}
		return new String(ch);
	}

	/**
	 * 检测字母字符串的 全小写1, 全大写2 ,首字母大写 3,异常-1
	 * 
	 * @param str
	 */
	public static int checkCode(String str) {
		int res = ERROR;
		Pattern pattern1 = Pattern.compile("[a-z]*");
		Pattern pattern2 = Pattern.compile("[A-Z]*");
		char[] ch = str.toCharArray();
		if (pattern1.matcher(str).matches()) {
			res = All_LOWERCASE;
		} else if (pattern2.matcher(str).matches()) {
			res = All_UPPERCASE;
		} else {
			for (int i = 0; i < ch.length; i++) {
				if (i == 0 & Character.isUpperCase(ch[0])) {
					res = FIRST_UPPERCASE;
				} else if (Character.isUpperCase(ch[i])) {
					res = ERROR;
				}
			}
		}
		return res;
	}

	/**
	 * 数组转字符串
	 * 
	 * @param aryStr
	 * @return
	 */
	public static String ArryToStr(String aryStr[]) {
		StringBuilder sbf = new StringBuilder();
		for (String str : aryStr) {
			if (str != null) {
				sbf.append(str);
			}
		}
		return sbf.toString();
	}

	public static String ListToStr(List<String> list) {
		StringBuilder sbf = new StringBuilder();
		for (String str : list) {
			if (str != null) {
				sbf.append(str);
			}
		}
		return sbf.toString();
	}

	/**
	 * char字符转String数组
	 * 
	 * @param arryChar
	 * @return
	 */
	public static String[] charArryToStringArry(char arryChar[]) {
		String[] arry = new String[arryChar.length];
		for (int i = 0; i < arryChar.length; i++) {
			arry[i] = String.valueOf(arryChar[i]);
		}
		return arry;
	}

	/**
	 * 删除 部分字符
	 * 
	 * @param str
	 * @param startPoint
	 * @param endPoint
	 * @return
	 */
	public static String delStr(String str, int startPoint, int endPoint) {
		StringBuilder sbf = new StringBuilder();
		String ary[] = StringUtils.charArryToStringArry(str.toCharArray());
		for (int i = 0; i < ary.length; i++) {
			if (i < startPoint || i > endPoint) {
				sbf.append(ary[i]);
			}
		}
		return sbf.toString();
	}

	/**
	 * 删除 部分字符
	 * 
	 * @param str
	 * @param startPoint
	 * @param endPoint
	 * @return
	 */
	public static String CoverStr(String str, int startPoint, int endPoint, String coverFlag) {
		StringBuilder sbf = new StringBuilder();
		String ary[] = StringUtils.charArryToStringArry(str.toCharArray());
		for (int i = 0; i < ary.length; i++) {
			if (i >= startPoint && i <= endPoint) {
				sbf.append(coverFlag);
			} else {
				sbf.append(ary[i]);
			}
		}
		return sbf.toString();
	}

	private static boolean isMatch(String regex, String orginal) {
		if (orginal == null || orginal.trim().equals("")) {
			return false;
		}
		Pattern pattern = Pattern.compile(regex);
		Matcher isNum = pattern.matcher(orginal);
		return isNum.matches();
	}

	/**
	 * 判断纯数字
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isNum(String str) {
		return isMatch("^\\d+$", str);
	}

	/**
	 * 判断纯字母
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isLatter(String str) {
		return isMatch("[a-zA-Z]+", str);
	}

	/**
	 * 判断正整数数字
	 * 
	 * @param orginal
	 * @return
	 */
	public static boolean isPositiveInteger(String orginal) {
		return isMatch("^\\+{0,1}[1-9]\\d*", orginal);
	}

	/**
	 * 判断负整数数字
	 * 
	 * @param orginal
	 * @return
	 */
	public static boolean isNegativeInteger(String orginal) {
		return isMatch("^-[1-9]\\d*", orginal);
	}

	/**
	 * 判断所有带符号的整数
	 * 
	 * @param orginal
	 * @return
	 */
	public static boolean isWholeNumber(String orginal) {
		return isMatch("[+-]{0,1}0", orginal) || isPositiveInteger(orginal) || isNegativeInteger(orginal);
	}

	/**
	 * 判断所有正数
	 * 
	 * @param orginal
	 * @return
	 */
	public static boolean isPositiveDecimal(String orginal) {
		return isMatch("\\+{0,1}[0]\\.[1-9]*|\\+{0,1}[1-9]\\d*\\.\\d*", orginal);
	}

	public static boolean isNegativeDecimal(String orginal) {
		return isMatch("^-[0]\\.[1-9]*|^-[1-9]\\d*\\.\\d*", orginal);
	}

	public static boolean isDecimal(String orginal) {
		return isMatch("[-+]{0,1}\\d+\\.\\d*|[-+]{0,1}\\d*\\.\\d+", orginal);
	}

	// 所有待符号的数字
	public static boolean isRealNumber(String orginal) {
		return isWholeNumber(orginal) || isDecimal(orginal);
	}

	public static boolean isContantSymbol(String str) {
		// TODO Auto-generated method stub
		if (str.replaceAll("[\u4e00-\u9fa5]*[a-z]*[A-Z]*\\d*-*_*\\s*", "").length() == 0) {
			// 如果不包含特殊字符
			return false;
		}
		return true;
	}

	public static int StrFlagNum(String str, String flag) {
		int i = 0;
		String ary[] = StringUtils.charArryToStringArry(str.toCharArray());
		for (String str1 : ary) {
			if (str1.equals(flag)) {
				i++;
			}
		}
		return i;
	}

	/**
	 * 判断字符串是数字和大写字母组合
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isUpcaseOrNum(String str) {
		String regExp = "^([0-9]+)|([A-Z]+)$";
		Pattern pat = Pattern.compile(regExp);
		Matcher mat = pat.matcher(str);
		return mat.matches();
	}

	/**
	 * 取大写字母 或者 数字
	 * 
	 * @return
	 */
	public static String getRandomUpcateOrNum() {
		Random random = new Random();
		boolean isNum = random.nextBoolean();
		if (isNum) {
			// 数字
			return String.valueOf(random.nextInt(10));
		} else {
			// 大写字母
			return String.valueOf((char) (65 + random.nextInt(26))); // 取得大写字母
		}
	}

	/**
	 * 根据Unicode编码完美的判断中文汉字和符号
	 * 
	 * @param c
	 * @return
	 */
	public static boolean isChinese(String str) {
		char[] array = str.toCharArray();
		for (char c : array) {
			if (!isChinese(c)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 根据Unicode编码完美的判断中文汉字和符号
	 * 
	 * @param c
	 * @return
	 */
	public static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
			return true;
		}
		return false;
	}

	/**
	 * string 转 unicode
	 * 
	 * @param s
	 * @return
	 */
	public static String string2Unicode(String s) {
		try {
			StringBuffer out = new StringBuffer("");
			byte[] bytes = s.getBytes("unicode");
			for (int i = 2; i < bytes.length - 1; i += 2) {
				out.append("u");
				String str = Integer.toHexString(bytes[i + 1] & 0xff);
				for (int j = str.length(); j < 2; j++) {
					out.append("0");
				}
				String str1 = Integer.toHexString(bytes[i] & 0xff);
				out.append(str);
				out.append(str1);
				out.append(" ");
			}
			return out.toString().toUpperCase();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * unicode 转 string
	 * 
	 * @param unicodeStr
	 * @return
	 */
	public static String unicode2String(String unicodeStr) {
		StringBuffer sb = new StringBuffer();
		String str[] = unicodeStr.toUpperCase().split("U");
		for (int i = 0; i < str.length; i++) {
			if (str[i].equals(""))
				continue;
			char c = (char) Integer.parseInt(str[i].trim(), 16);
			sb.append(c);
		}
		return sb.toString();
	}

	public static int getWordCount(String str, String word) {
		int x = 0;
		// 遍历数组的每个元素
		for (int i = 0; i <= str.length() - 1; i++) {
			String getstr = str.substring(i, i + 1);
			if (getstr.equals(word)) {
				x++;
			}
		}
		return x;
	}

	public static String trimAll(String str) {
		return str.trim().replace(" ", "");
	}

	/**
	 * 转成逗号连接的字符串
	 * 
	 * @param str
	 * @return
	 */
	public static String arry2String(String[] str) {
		StringBuffer sb = new StringBuffer();
		if (str == null) {
			return null;
		}
		for (String string : str) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(string);
		}
		return sb.toString();
	}
}
