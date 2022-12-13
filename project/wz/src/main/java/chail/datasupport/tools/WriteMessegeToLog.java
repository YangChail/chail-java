
package chail.datasupport.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class WriteMessegeToLog {
	private static Calendar cale;
	private static Date tasktime;
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static List<String> logs=new ArrayList<>();

	public static void writeToLog(String messege,String fileName) {
		logs.add(messege);
		String logFilePath = System.getProperty("user.dir") + File.separator
				+ fileName;
		File file = new File(logFilePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(fw);
			cale = Calendar.getInstance();
			tasktime = cale.getTime();
			System.out.println(messege);
			bw.write(messege);
			bw.newLine();

			bw.flush();
			bw.close();
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
