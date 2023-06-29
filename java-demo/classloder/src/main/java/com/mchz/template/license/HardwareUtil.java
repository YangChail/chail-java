package com.mchz.template.license;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class HardwareUtil {
  public static String getOsName() {
    String os = "";
    os = System.getProperty("os.name");
    return os;
  }
  
  public static String getCPUSerialTest() {
    String serial = "";
    try {
      Process process = Runtime.getRuntime().exec(new String[] { "wmic", "cpu", "get", "ProcessorId" });
      process.getOutputStream().close();
      Scanner sc = new Scanner(process.getInputStream());
      serial = sc.next();
    } catch (IOException e) {
      e.printStackTrace();
    } 
    return serial;
  }
  
  public static String getCPUSerial() {
    String result = "";
    String os = getOsName();
    if (os.startsWith("Windows")) {
      try {
        File file = File.createTempFile("tmp", ".vbs");
        file.deleteOnExit();
        FileWriter fw = new FileWriter(file);
        String vbs = "On Error Resume Next \r\n\r\nstrComputer = \".\"  \r\nSet objWMIService = GetObject(\"winmgmts:\" _ \r\n    & \"{impersonationLevel=impersonate}!\\\\\" & strComputer & \"\\root\\cimv2\") \r\nSet colItems = objWMIService.ExecQuery(\"Select * from Win32_Processor\")  \r\n For Each objItem in colItems\r\n     Wscript.Echo objItem.ProcessorId  \r\n     exit for  ' do the first cpu only! \r\nNext                    ";
        fw.write(vbs);
        fw.close();
        Process p = Runtime.getRuntime().exec("cscript //NoLogo " + file.getPath());
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = input.readLine()) != null)
          result = result + line; 
        input.close();
        file.delete();
      } catch (Exception e) {
        e.fillInStackTrace();
      } 
    } else if (os.startsWith("Linux")) {
      String CPU_ID_CMD = "dmidecode -t 4 | grep ID |sort -u |awk -F': ' '{print $2}'";
      try {
        Process p = Runtime.getRuntime().exec(new String[] { "sh", "-c", CPU_ID_CMD });
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        if ((line = br.readLine()) != null)
          result = result + line; 
        br.close();
      } catch (IOException iOException) {}
    } 
    return result.trim();
  }
  
  public static void main(String[] args) throws Exception {
    String cpuSerial = getCPUSerial();
    String test = cpuSerial;
    System.out.println(test);
  }
}
