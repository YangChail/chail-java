package com.mchz.template.license;

import com.mchz.template.license.CreateLicense;

import javax.xml.crypto.Data;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Test3 {


    public static void main(String[] args) throws Exception {

        DateFormat simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.add(5, 60);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = sdf.format(cal.getTime());
        CreateLicense jiami = new CreateLicense();
        List<String> srcstr = new ArrayList<>();
        srcstr.add("0" + "|" + "99" + "|" + dateString);
        Date now = new Date();
        License ruleLicenseDate =new License();
        ruleLicenseDate.setHostId("0");
        ruleLicenseDate.setDbNum(99);
        ruleLicenseDate.setEndDate( sdf.parse(dateString));
        ruleLicenseDate.setVerifyValue(jiami.createLicense(srcstr, "capaa", null, "private.key")[0].toString());
        ruleLicenseDate.setUpdateTime(now);
        ruleLicenseDate.setKeyName("DM");
        ruleLicenseDate.setRegistDate(now);
        ruleLicenseDate.setMd5Value(ruleLicenseDate.toMd5String());
        StringBuffer sbf=new StringBuffer();
        sbf.append(" update mc_license");
        sbf.append(" set regist_date= '"+ DateUtils.date2String2(ruleLicenseDate.getRegistDate())+"',");
        sbf.append("  end_date= '"+DateUtils.date2String2(ruleLicenseDate.getEndDate()) +"', ");
        sbf.append("  host_id='"+ruleLicenseDate.getHostId()+"', ");
        sbf.append("  db_num="+ruleLicenseDate.getDbNum()+" ,");
        sbf.append("  verify_value= '"+ruleLicenseDate.getVerifyValue()+"', ");
        sbf.append("  insert_time= now() ,");
        sbf.append("  update_time= now(), ");
        sbf.append("  md5_value= '"+ruleLicenseDate.toMd5String()+"' ");
        sbf.append(" where key_name = 'DM'");
        System.out.println(sbf.toString());
    }




}
