package com.mcha.bigdata;

import com.mchz.bigdata.hbase.HBaseConn;
import com.mchz.bigdata.hbase.HBaseTable;
import com.mchz.bigdata.hbase.HBaseUtil;
import com.mchz.bigdata.hdfs.utils.HdfsUtil;
import com.mchz.bigdata.hdfs.utils.KerboersUtils;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MT2 {
	private static final String CONFIG_PATH = System.getProperty("user.dir")+ File.separator +"config";
	public  static HBaseConn init(String zkInfo) throws IOException {
		Properties properties =new Properties();
		properties.setProperty(HdfsUtil.CORE_SITE_FILE_NAME, getProPath("core-site.xml"));
		properties.setProperty(HdfsUtil.HBASE_SITE_FILE_NAME, getProPath("hbase-site.xml"));
		properties.setProperty(HdfsUtil.HDFS_SITE_NAME, getProPath("hdfs-site.xml"));
		properties.setProperty(KerboersUtils.KEY_DM_KERBEROS_PRINCIPAL,getPrincipalNames(getProPath("user.keytab")) );
		properties.setProperty(KerboersUtils.KEY_DM_KERBEROS_KEYTAB, getProPath("user.keytab"));
		properties.setProperty(KerboersUtils.KEY_DM_KERBEROS_KRB5_CONF, getProPath("krb5.conf"));
		properties.setProperty(KerboersUtils.DM_HIVE_KERBEROS_ENABLE, "true");
		HBaseConn hbaseConn = new HBaseConn(zkInfo,properties);
		return hbaseConn;
	}


	/**
	 * 监测principal
	 *
	 * @param keytabFileName
	 * @return
	 * @throws IOException
	 */
	public static final String getPrincipalNames(String keytabFileName) throws IOException {
		Keytab keytab = Keytab.read(new File(keytabFileName));
		List<String> principals = new ArrayList<>();
		List<KeytabEntry> entries = keytab.getEntries();
		if(entries.size()<1){
			throw new IOException("读取keytab错误"+keytabFileName);
		}
		for (KeytabEntry entry : entries) {
			principals.add(entry.getPrincipalName().replace("\\", "/"));
		}
		return principals.get(0);
	}


	private static String getProPath(String fileName){
		return CONFIG_PATH+ File.separator+fileName;
	}


	//wbh.stu
	public static void main(String[] args) throws Exception {
		HBaseConn hbaseConn = init("172.16.67.3:24002,172.16.67.4:24002");
		HBaseUtil hBaseUtil=new HBaseUtil(hbaseConn);
		List<HBaseTable> allTables = hBaseUtil.getAllTables();
		for(HBaseTable hb:allTables){
			System.out.println(hb.toString());
		}



	}

}
