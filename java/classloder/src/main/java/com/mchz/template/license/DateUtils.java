package com.mchz.template.license;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author hukp
 *
 */
public class DateUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(DateUtils.class);
	private static String a = "yyyy-MM-dd";

	public static final String b = " 23:59:59";
	public static final String c = " 00:00:00";
	private static Pattern DATE_PATTERN = Pattern
			.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1-2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");


	public static class Formats {

		// 完整时间
		private final DateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// 年月日
		private final DateFormat dtSimple = new SimpleDateFormat(a);
        //年月
		public final DateFormat monthSimple = new SimpleDateFormat("yyyy-MM");
		//月日
		private final DateFormat daySimple = new SimpleDateFormat("MM-dd");

		private final DateFormat dtSimple2 = new SimpleDateFormat("yyyyMMdd");

		public final DateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		private final DateFormat dtSimpleLong = new SimpleDateFormat("yyyyMMddHHmmss");

		// 年月日
		private final DateFormat dtSimpleChinese = new SimpleDateFormat("yyyy年MM月dd日");

		// 年月日
		private final DateFormat dtChinese = new SimpleDateFormat("yyyy年MM月dd日 HH点mm分");

		//private final SimpleDateFormat formatter = new SimpleDateFormat("dd天 HH:mm:ss");//初始化Formatter的转换格式。
		private final DateFormat dtUnderLine = new SimpleDateFormat("yyy_MM_dd_HH_mm_ss");

		public final DateFormat timeFrameFormat = new SimpleDateFormat("yyy-MM-dd-HH");
		public final DateFormat dayAndMonthFormat = new SimpleDateFormat("yyy_MM");
	}

	private static final ThreadLocal<Formats> LOCAL = new ThreadLocal<Formats>();

	public static Formats getFormats() {
		Formats f = LOCAL.get();
		if (f == null) {
			f = new Formats();
			LOCAL.set(f);
		}
		LOCAL.remove();
		return f;
	}

	private static Calendar getCal(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal;
	}

	/**
	 * 返回yyyy-MM-dd HH:mm:ss完整时间
	 *
	 * @param date
	 *            null取当前时间
	 *
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static final String simpleFormat(Date date) {
		if (date == null) {
			date = new Date();
		}

		return getFormats().simple.format(date);
	}

	/**
	 * 返回yyyy-MM-dd
	 *
	 * @param date
	 *
	 * @return yyyy-MM-dd
	 */
	public static final String dtSimpleFormat(Date date) {
		if (date == null) {
			return getCurday();
		}
		return getFormats().dtSimple.format(date);
	}

	/**
	 * yyyy-MM-dd 日期字符转换为时间
	 *
	 * @param stringDate
	 *            yyyy-MM-dd
	 *
	 * @return date
	 *
	 */
	public static final Date string2Date(String stringDate) {

		if (stringDate == null || stringDate.equals("")) {
			return null;
		}
		try {
			return getFormats().dtSimple.parse(stringDate);
		} catch (ParseException e) {
			return new Date();
		}
	}

	/**
	 * 返回日期时间
	 *
	 * @param stringDate
	 *            yyyy-MM-dd HH:mm:ss
	 *
	 * @return date 日期
	 *
	 * @throws ParseException
	 */
	public static final Date string2DateTime(String stringDate) throws ParseException {
		if (stringDate == null || stringDate.equals("")) {
			return null;
		}
		return getFormats().simple.parse(stringDate);
	}

	/**
	 * 返回日期时间
	 *
	 * @param stringDate
	 *            yyyyMMddHHmmss
	 *
	 * @return date 日期
	 *
	 * @throws ParseException
	 */
	public static final Date string2DateLong(String stringDate) throws ParseException {
		if (stringDate == null || stringDate.equals("")) {
			return null;
		}

		return getFormats().dtSimpleLong.parse(stringDate);
	}

	/**
	 * 时间转换字符串
	 *
	 * @param date
	 *
	 * @return yyyy-MM-dd HH:mm
	 */
	public static final String simpleDate(Date date) {
		if (date == null) {
			date = new Date();
		}
		return getFormats().simpleFormat.format(date);
	}

	/**
	 * yyyy年MM月dd日 日期字符转换为时间
	 *
	 * @param stringDate
	 *
	 * @return Date
	 *
	 * @throws ParseException
	 */
	public static final Date chineseString2Date(String stringDate) {
		if (stringDate == null) {
			return null;
		}
		try {
			return getFormats().dtSimpleChinese.parse(stringDate);
		} catch (ParseException e) {
			return null;
		}

	}

	/**
	 * yyyy年MM月dd日 参数空为当前时间
	 *
	 * @param date
	 *
	 * @return string
	 */
	public static final String dtSimpleChineseFormat(Date date) {
		if (date == null) {
			date = new Date();
		}
		return getFormats().dtSimpleChinese.format(date);
	}

	/**
	 * yyyy年MM月dd日 HH点MM分 参数空为当前时间
	 *
	 * @param date
	 *
	 * @return string
	 */
	public static final String dtChineseFormat(Date date) {
		if (date == null) {
			date = new Date();
		}
		return getFormats().dtChinese.format(date);
	}

	/**
	 *
	 * @return 当年
	 */
	public static String getYear() {
		return String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
	}

	/**
	 *
	 * @return 当前日期 yyyy-MM-dd
	 */
	public static String getCurday() {
		String datime = getFormats().dtSimple.format(new Date());
		return datime;
	}

	/**
	 *
	 * @return 当前日期 yyyyMMdd
	 */
	public static String getCurday2() {
		String datime = getFormats().dtSimple2.format(new Date());
		return datime;
	}

	public static String getCurday3() {
		String datime = getFormats().dtSimpleLong.format(new Date());
		return datime;
	}

	/**
	 * date 转为 string
	 *
	 * @param date
	 * @return yyyy-MM-dd
	 */
	public static String date2String(Date date) {
		String p = getFormats().dtSimple.format(date);
		return p;
	}

	/**
	 * date 转为yyyy-MM-dd HH:mm:ss
	 *
	 * @param date
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static String date2String2(Date date) {
		String p = getFormats().simple.format(date);
		return p;
	}

	/**
	 * date 转为yyyyMMdd
	 *
	 * @param date
	 * @return yyyyMMdd
	 */
	public static String date2String3(Date date) {
		String p = getFormats().dtSimple2.format(date);
		return p;
	}

	/**
	 * 加天数
	 *
	 * @param date
	 * @param x
	 *            增加的天数 可为负
	 * @return 增加x天后的date
	 */
	public static Date addDay(Date date, int x) {
		Calendar cal = getCal(date);
		cal.add(Calendar.DAY_OF_MONTH, x);
		Date date2 = cal.getTime();
		return date2;
	}

	/**
	 * 增加天数
	 *
	 * @param time
	 *            时间字符串
	 * @param x
	 *            天数
	 * @return 增加x天后的date
	 * @throws ParseException
	 */
	public static Date addDay(String time, int x) throws ParseException {
		Date date = string2Date(time);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, x);
		return cal.getTime();
	}

	/**
	 * 增加月数
	 *
	 * @param time
	 *            时间字符串
	 * @param x
	 *            月数
	 * @return 增加x月后的date
	 * @throws ParseException
	 */
	public static Date addMon(String time, int x) throws ParseException {
		Date date = string2Date(time);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MONTH, x);
		return cal.getTime();
	}

	/**
	 * 加月数
	 *
	 * @param date
	 * @param x
	 *            增加的月数可为负
	 * @return 增加x月后的date
	 */
	public static Date addMonth(Date date, int x) {
		Calendar cal = getCal(date);
		cal.add(Calendar.MONTH, x);
		Date date2 = cal.getTime();
		return date2;
	}

	/**
	 * 加年数
	 *
	 * @param date
	 * @param x
	 *            增加的月数可为负
	 * @return 增加x月后的date
	 */
	public static Date addYear(Date date, int x) {
		Calendar cal = getCal(date);
		cal.add(Calendar.YEAR, x);
		Date date2 = cal.getTime();
		return date2;
	}


	/**
	 *
	 * description : 得到日期所在周的下周一
	 *
	 * @param date
	 *            日期
	 * @return 下周一
	 *
	 * @author : cq
	 *
	 */
	public static Date getNextMonday(String date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setTime((string2Date(date)));
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6); // Sunday
		return addDay(c.getTime(), 1);
	}

	/**
	 *
	 * description : 得到日期所在周的周n
	 *
	 * @param date
	 *            日期
	 * @param n
	 *            周几
	 * @return Date
	 *
	 * @author : cq
	 *
	 */
	public static Date getWeekDay(String date, int n) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setTime(string2Date(date));
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + n - 1);
		return c.getTime();
	}

	/**
	 * 返回日期所在的月 增加mon个月后的月份
	 *
	 * @param dt
	 *            指定date
	 * @param mon
	 *            增加的月数 可为负
	 * @return YYYY-MM
	 */
	public static final String getNextMon(String dt, int mon) {
		if (dt == null) {
			return null;
		}
		Calendar c = Calendar.getInstance();
		c.setTime((string2Date(dt)));
		c.add(Calendar.MONDAY, mon);
		String year = String.valueOf(c.get(Calendar.YEAR));
		int month = c.get(Calendar.MONTH) + 1;
		String sMonth;
		if (month < 10) {
			sMonth = "0" + String.valueOf(month);
		} else {
			sMonth = String.valueOf(month);
		}
		return year + "-" + sMonth;
	}

	/**
	 * 获取报表计算时间戳 返回年-月-日-时
	 *
	 * @param date
	 * @return
	 */
	public static String getTimeFrame(Date date, String type) {
		StringBuffer dateStr = new StringBuffer();
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		return dateStr.toString();
	}

	public static Date str2Date(String dateStr, DateFormat formate) {
		try {
			return formate.parse(dateStr);
		} catch (ParseException e) {
			LOGGER.error("日期字符错误", e);
		}
		return null;
	}

	/**
	 * 获取输入时间的月份的第一天
	 *
	 * @param dt
	 * @return string
	 */
	public static final Date getMonthFirstDay(String dt) {
		if (dt == null) {
			return null;
		}
		Calendar c = Calendar.getInstance();
		c.setTime(string2Date(dt));
		c.set(Calendar.DAY_OF_MONTH, 1);
		return c.getTime();
	}

	/**
	 * 获取输入时间的月份的最后一天
	 *
	 * @param dt
	 * @return Date
	 */
	public static final Date getMonthLastDay(String dt) {
		if (dt == null) {
			return null;
		}
		Calendar c = Calendar.getInstance();
		c.setTime(string2Date(dt));
		c.add(Calendar.MONTH, 1);
		c.set(Calendar.DAY_OF_MONTH, 1);
		return addDay(c.getTime(), -1);
	}

	/**
	 * 相差天数
	 *
	 * @param start
	 *            Date
	 * @param endDate
	 *            Date
	 * @return int
	 */
	public static int getBetweenDate(Date start, Date endDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(endDate);
		int end = c.get(Calendar.YEAR) * 365 + c.get(Calendar.DAY_OF_YEAR);
		c.setTime(start);
		return end - (c.get(Calendar.YEAR) * 365 + c.get(Calendar.DAY_OF_YEAR));
	}

	/**
	 * 取与当前时间相差n天的时间yyyy-MM-dd HH:mm:ss
	 *
	 * @param n
	 *            负:向前 正:向后 0:当前时间
	 * @return str
	 */
	public static String getTimeBetweenNow(Integer n) {
		Date cur = new Date();
		Calendar c = Calendar.getInstance();
		c.setTime(cur);

		c.add(Calendar.DAY_OF_MONTH, n);
		String oneDay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
		return oneDay;
	}

	/**
	 * 取上个月的第一天或最后一天
	 *
	 * @param flag
	 *            false：第一天；true：最后一天
	 * @return
	 */
	public static String getDayByMonth(boolean flag, Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.DAY_OF_MONTH, 1);
		String oneDay = "";

		if (flag) {
			c.add(Calendar.DAY_OF_MONTH, -1);
			oneDay = new SimpleDateFormat(a).format(c.getTime()) + b;
		} else {
			c.add(Calendar.MONDAY, -1);
			oneDay = new SimpleDateFormat(a).format(c.getTime()) + c;
		}
		return oneDay;
	}

	/**
	 * 取上周的第一天或者最后一天
	 *
	 * @param flag
	 *            true：第一天；false：最后一天
	 * @return
	 *//*
	public static String getDayByWeek(boolean flag) {
		Calendar cal = Calendar.getInstance();
		// 获取当前时间
		long today = cal.getTimeInMillis();
		// 设定周日是每周的第一天
		cal.setFirstDayOfWeek(Calendar.SUNDAY);
		// 得到当天处在当周的第几天，周日是当周的第一天
		int dayNum = cal.get(Calendar.DAY_OF_WEEK);
		if (dayNum == 1) {
			if (!flag) {
				cal.setTimeInMillis(today + (7 - dayNum) * 24 * 60 * 60 * 1000);
			}
		} else if (dayNum > 1 && dayNum <= 7) {
			if (flag) {
				cal.setTimeInMillis(today - (dayNum - 2 + 7) * 24 * 60 * 60 * 1000);
			} else {
				cal.setTimeInMillis(today + (7 - dayNum - 6) * 24 * 60 * 60 * 1000);
			}
		}
		String oneDay;
		if (flag){
			oneDay = new SimpleDateFormat(a).format(cal.getTime()) + c;
		}
		else{
			oneDay = new SimpleDateFormat(a).format(cal.getTime()) + b;
		}
		return oneDay;
	}*/

	/**
	 * 取上一天的起止时间
	 *
	 * @param flag
	 *            true：开始时间；false：结束时间
	 * @return
	 */
	public static String getPreivesDay(boolean flag) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1); // 得到前一天
		Date date = calendar.getTime();
		DateFormat df = new SimpleDateFormat(a);
		String oneDay = df.format(date);
		if (flag){
			oneDay += c;
		}
		else{
			oneDay += b;
		}
		return oneDay;
	}

	public static Date addHours(Date date, int x) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, x);
		return cal.getTime();
	}

	public static int getHours(Date date) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.HOUR_OF_DAY);
	}

	public static int getDay(Date date) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.DATE);
	}

	public static int getMonth(Date date) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.MONTH) + 1;
	}
	/**
	 * 与输入时间相差num分钟的时间 正数向后 负数向前
	 *
	 * @param time
	 * @param num
	 * @return Date
	 */
	public static Date addMinuteDate(String time, int num) {
		Date date = null;
		try {
			date = string2DateTime(time);
		} catch (ParseException e) {
			LOGGER.error("输入时间错误", e);
		}
		Calendar cal = Calendar.getInstance();
		if(cal!=null) {
			cal.setTime(date);
			cal.add(Calendar.MINUTE, num);
			return cal.getTime();
		}
		return null;
	}

	/**
	 * 与输入时间相差num分钟的时间 正数向后 负数向前
	 *
	 * @param time
	 * @param num
	 * @return str yyyy-MM-dd HH:mm:ss
	 */
	public static String addMinuteStr(String time, int num) {
		return date2String2(addMinuteDate(time, num));
	}

	/**
	 * 与输入时间相差seconds秒的时间 正数向后 负数向前
	 *
	 * @param date
	 * @param seconds
	 * @return Date
	 */
	public static Date addSecond(Date date, int seconds) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.SECOND, seconds);
		return cal.getTime();
	}

	/**
	 *
	 * description : 得到本周的周n
	 *
	 * @param n
	 *            周几
	 * @return Date
	 *
	 * @author : cq
	 *
	 */
	public static String getCurWeekDay(int n) {
		Calendar c = Calendar.getInstance();
		if (n % 7 == 0) {// 周日
			c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6);
			c.add(Calendar.DATE, 1);
		} else {
			c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + n);
		}

		return dtSimpleFormat(c.getTime());
	}

	/**
	 * 比较时间 是否 small早于big
	 *
	 * @param small
	 *            yyyy-mm-dd hh:mm
	 * @param big
	 *            yyyy-mm-dd hh:mm
	 * @return true small<big else false
	 */
	public static boolean isAfterTime(String small, String big, DateFormat format) {
		if(format == null) {
			format = getFormats().simpleFormat;
		}
		boolean r = false;
		try {
			Date date1 = format.parse(small);
			Date date2 = format.parse(big);
			if (date1.before(date2)) {
				r = true;
			}
		} catch (ParseException e) {
			LOGGER.error("输入格式错误", e);
		}

		return r;
	}

	public static boolean isAfterTime(String small, String big) {
		boolean r = false;
		try {
			Date date1 = getFormats().simpleFormat.parse(small);
			Date date2 = getFormats().simpleFormat.parse(big);
			if (date1.before(date2)) {
				r = true;
			}
		} catch (ParseException e) {
			LOGGER.error("输入格式错误", e);
		}

		return r;
	}

	public static String formatDuring(long mss) {
		StringBuilder stringBuilder = new StringBuilder();
		long days = mss / (1000 * 60 * 60 * 24);
		long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
		long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
		long seconds = (mss % (1000 * 60)) / 1000;
		if (days != 0) {
			stringBuilder.append(days);
			stringBuilder.append(" 天 ");
		}
		if (days != 0 || hours != 0) {
			stringBuilder.append(hours);
			stringBuilder.append(" 小时 ");
		}
		if (hours != 0 || minutes != 0) {
			stringBuilder.append(minutes);
			stringBuilder.append(" 分钟 ");
		}
		stringBuilder.append(seconds);
		stringBuilder.append(" 秒 ");
		return stringBuilder.toString();
//		return days + " 天 " + hours + " 小时 " + minutes + " 分钟 "
//				+ seconds + " 秒 ";
	}
	public static Date getTodayBeginTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return calendar.getTime();
	}

	public static Date getOneDayBeginTime(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return calendar.getTime();
	}

	/**
	 * 下划线格式的时间字符串
	 * @param date
	 * @return
	 */
	public static final String getUnderLineTime(Date date) {
		if (date == null) {
			date = new Date();
		}
		return getFormats().dtUnderLine.format(date);
	}



	public static void main(String[] args) throws ParseException {
	}

	public static Calendar setTime(Calendar calendar, int hour, int minute, int second) {
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		return calendar;
	}

	/**
	 * 是否在区间内
	 * @param nowTime
	 * @param beginTime
	 * @param endTime
	 * @return
	 */
	public static boolean belongCalendar(Date nowTime, Date beginTime, Date endTime) {
	    return nowTime.getTime() >= beginTime.getTime() && nowTime.getTime() < endTime.getTime();
	}

	/**
	 * 验证字符串日期是否合法，月份或天数是否正确
	 * @param str
	 * @return
	 */
	public static boolean isValidDate(String str) {
		boolean convertSuccess = true;
		try {
			// 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
			getFormats().dtSimple.setLenient(false);
			Date dateTrans = getFormats().dtSimple.parse(str);
			String date2 = getFormats().dtSimple.format(dateTrans);
			return date2.equals(str);
		} catch (ParseException e) {
			// e.printStackTrace();
			// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
			convertSuccess = false;
		}

		return convertSuccess;
	}

	/**
	 * 正则表达式验证日期
	 * @param strDate
	 * @return
	 */
	public static boolean isDate(String strDate) {
        Matcher m = DATE_PATTERN.matcher(strDate);
        if (m.matches()) {
            return true;
        } else {
            return false;
        }
	}
}
