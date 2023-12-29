
package org.chail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WriteMessegeToLog {
    private static Calendar cale;
    private static Date tasktime;
    private static SimpleDateFormat df = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");
    ;

    public static void writeToLog(String messege, String fileName) {
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


    public static void clear(String logFilePath) {
        File file = new File(logFilePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            } else {
                file.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
