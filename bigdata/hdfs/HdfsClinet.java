package com.chail.apputil.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.util.internal.ConcurrentHashMap;

public class HdfsClinet {

	public static void main(String[] args) throws Exception {

		//getFileNomal();
		//getfiles7();
		//getFileNomal();
	}
	
	
	public static void getfiles1() throws IOException, URISyntaxException {
		String user = "hive/hadoop.hadoop.com@HADOOP.COM";
		String keytab = "D:/hive.keytab";
		String confStr="D:/krb5.conf";
		String dir = "hdfs://192.168.200.18:8020/";
		Configuration conf = new Configuration();
		// conf.addResource(new Path("D:/hdfs-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		// conf.addResource(new Path("D:/core-site.xml"));
		System.setProperty("java.security.krb5.conf", "D:/hadoop-conf/18/krb5.conf");
		System.setProperty("HADOOP_USER_NAME", "hive");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(user, keytab);
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("fs.default.name", dir);

		FileSystem fs = FileSystem.get(new URI(dir), conf);
		Path path = new Path(dir);
		fs.exists(path);
		System.out.println("READING ============================");
		
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			System.out.println(fileStatus.getPath());
		}
	}
	
	
	
	
	
	
	

	private static void getFileNomal() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		UserGroupInformation.setConfiguration(conf);
		String filePath = "hdfs://192.168.239.2/";
		Path path = new Path(filePath);
		FileSystem fs = FileSystem.get(new URI(filePath), conf);
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			System.out.println(Thread.currentThread().getName() + " 000000000000000000000 " + fileStatus.getPath());
		}
	}

	public static void getfiles() {
		try {
			Configuration conf = new Configuration();
			// 不设置该代码会出现错误：java.io.IOException: No FileSystem for scheme: hdfs
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			String filePath = "hdfs://192.168.210.183:8020/user";
			Path path = new Path(filePath);
			// 这里需要设置URI，否则出现错误：java.lang.IllegalArgumentException: Wrong FS:
			FileSystem fs = FileSystem.get(new URI(filePath), conf);
			System.out.println("READING ============================");
			FileStatus[] files = fs.listStatus(path);
			for (FileStatus fileStatus : files) {
				System.out.println(fileStatus.getPath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void getfiles2() throws Exception {
		String user = "hdfs/master@HADOOP.COM";
		String keytab = "D:/hadoop-conf/18/hdfs.keytab";
		String dir = "hdfs://hdfs:hdfs@192.168.200.18:8020/";
		Configuration conf = new Configuration();
		// conf.addResource(new Path("D:/hdfs-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		// conf.addResource(new Path("D:/core-site.xml"));
		System.setProperty("java.security.krb5.conf", "D:/hadoop-conf/18/krb5.conf");
		System.setProperty("HADOOP_USER_NAME", "hive");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(user, keytab);
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
	
	
		
		
		

		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("fs.default.name", dir);

		FileSystem fs = FileSystem.get(new URI(dir), conf);
		Path path = new Path(dir);
		fs.exists(path);
		System.out.println("READING ============================");
		
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			System.out.println(fileStatus.getPath());
		}
	}

	public static void getfiles3() throws Exception {
		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:/167hdfs.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		Configuration conf = new Configuration();
		// conf.addResource(new Path("D:/hdfs-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		// conf.addResource(new Path("D:/core-site.xml"));
		System.setProperty("java.security.krb5.conf", "D:/167krb5.conf");
//        System.setProperty("HADOOP_USER_NAME", "hive");
		UserGroupInformation.setConfiguration(conf);

		// UserGroupInformation.loginUserFromKeytab(user, keytab);
		// UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		KerberosUtil kb = new KerberosUtil();

		LoginContext loginContextFromKeytab = kb.getLoginContextFromKeytab(user, keytab);
		Subject subject = loginContextFromKeytab.getSubject();
		loginContextFromKeytab.login();
		UserGroupInformation.loginUserFromSubject(subject);

		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(new URI(dir), conf);

		System.out.println("READING ============================");
		Path path = new Path(dir);
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			System.out.println(fileStatus.getPath());
		}
	}

	public static void getfiles4() throws Exception {

		String user1 = "hdfs/master@HADOOP.COM";
		String keytab1 = "D:/hdfs.keytab";
		String dir1 = "hdfs://192.168.200.18:8020/";
		String confStr1 = "D:/krb5.conf";
		Subject login12 = login1(user1, keytab1, confStr1);
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromSubject(login12);
		printFile(dir1, conf);

		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:/hdfs167.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		String confStr = "D:/krb5167.conf";
		Subject login1 = login1(user, keytab, confStr);
		Configuration conf2 = new Configuration();
		conf2.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.loginUserFromSubject(login1);
		UserGroupInformation.setConfiguration(conf2);
		// UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
		System.setProperty("java.security.krb5.conf", confStr);
		printFile(dir, conf2);

		// UserGroupInformation.loginUserFromSubject(login12);
		System.setProperty("java.security.krb5.conf", confStr1);
		login1(user1, keytab1, confStr1);
		UserGroupInformation.loginUserFromSubject(login12);
		UserGroupInformation.setConfiguration(conf);
		printFile(dir1, conf);

		System.setProperty("java.security.krb5.conf", confStr);
		login1(user, keytab, confStr);
		UserGroupInformation.loginUserFromSubject(login1);
		printFile(dir, conf2);

		// printFile(dir1);
		// System.setProperty("java.security.krb5.conf", confStr);
		// UserGroupInformation.loginUserFromSubject(login1);

		System.out.println();

	}

	public static void getfiles5() throws Exception {
		ConcurrentHashMap<String, Subject> map = new ConcurrentHashMap<>(2);
		List<String> list = Collections.synchronizedList(new ArrayList<>());

		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:/hdfs167.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		String confStr = "D:/krb5167.conf";
		Subject s1 = login1(user, keytab, confStr);
		UserGroupInformation.loginUserFromSubject(s1);
		printFile(dir + "user");
		// printFile(dir);
		List<Exception> errorlist = Collections.synchronizedList(new ArrayList<>());
		list.add(new URI(dir).getAuthority());
		map.put(new URI(dir).getAuthority(), s1);
		String user1 = "hdfs/master@HADOOP.COM";
		String keytab1 = "D:/hdfs.keytab";
		String dir1 = "hdfs://192.168.200.18:8020/";
		String confStr1 = "D:/krb5.conf";
		Subject s2 = login1(user1, keytab1, confStr1);
		map.put(new URI(dir1).getAuthority(), s2);
		list.add(new URI(dir1).getAuthority());
		UserGroupInformation.loginUserFromSubject(s1);
		printFile(dir + "user");
		UserGroupInformation.loginUserFromSubject(s2);
		printFile(dir1 + "user");

		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		Random r = new Random();
		// int nextInt = 1;
//		for (int i = 0; i < 10000; i++) {
//			fixedThreadPool.submit(() -> {
//				String string2 = "";
//				String dd = "";
//				try {
//					int nextInt = 0;
//					synchronized (r) {
//						nextInt = r.nextInt(2);
//						System.out.println(nextInt);
//					}
//					String string = list.get(nextInt);
//					Subject subject = map.get(string);
//					string2 = subject.toString();
//					//UserGroupInformation.loginUserFromSubject(subject);
//					if (nextInt == 0) {
//						dd = dir;
//					}
//					if (nextInt == 1) {
//						dd = dir1;
//					}
//					printFile(dd);
//				} catch (IOException | URISyntaxException e) {
//					//errorlist.add(e);
//					//e.printStackTrace();
//					//System.out.println(string2);
//					//System.out.println(dd);
//
//				}
//
//			});
//		}

		// System.out.println(errorlist.size());

	}

	public static void getfiles6() throws Exception {

		ConcurrentHashMap<String, Subject> map = new ConcurrentHashMap<>(2);
		ConcurrentHashMap<String, Configuration> mapconf = new ConcurrentHashMap<>(2);
		List<String> list = Collections.synchronizedList(new ArrayList<>());

		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:\\hadoop-conf\\167\\hdfs.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		String confStr ="D:\\hadoop-conf\\167\\krb5.conf";
		Subject s0 = login1(user, keytab, confStr);
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromSubject(s0);
		printFile(dir + "user");
		list.add(new URI(dir).getAuthority());
		map.put(new URI(dir).getAuthority(), s0);
		String user1 = "hdfs/master@HADOOP.COM";
		String keytab1 = "D:\\hadoop-conf\\18\\hdfs.keytab";
		String dir1 = "hdfs://192.168.200.18:8020/";
		String confStr1 = "D:\\hadoop-conf\\18\\krb5.conf";
		Subject s1 = login1(user1, keytab1, confStr1);
		map.put(new URI(dir1).getAuthority(), s1);
		
		list.add(new URI(dir1).getAuthority());
		Configuration conf2 = new Configuration();
		conf2.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf2);
		UserGroupInformation.loginUserFromSubject(s1);
		printFile(dir1 + "user");
		mapconf.put(new URI(dir1).getAuthority(), conf);
		mapconf.put(new URI(dir).getAuthority(), conf2);
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		Random r = new Random();
		// int nextInt = 1;
		for (int i = 0; i < 10000; i++) {
			fixedThreadPool.submit(() -> {
				String string2 = "";
				String dd = "";
				try {
					int nextInt = 0;
					synchronized (r) {
						nextInt = r.nextInt(2);
						System.out.println(nextInt);
					}
					String string = list.get(nextInt);
					Subject subject = map.get(string);
					string2 = subject.toString();
					UserGroupInformation.loginUserFromSubject(subject);
//					FileSystem fs = FileSystem.get(new URI(dir), mapconf.get(new URI(dir)));
//					System.out.println(dir+"==========="+fs.exists(new Path("/user")));
					
					if (nextInt == 0) {
						dd = dir;
					}
					if (nextInt == 1) {
						dd = dir1;
					}
					printFile(dd);
				} catch (IOException | URISyntaxException e) {
					System.out.println("=========Error============" + Thread.currentThread().getName());
					// errorlist.add(e);
					// e.printStackTrace();
					// System.out.println(string2);
					// System.out.println(dd);

				}

			});
		}
	}
	
	
	
	public static void getfiles7() throws Exception {

		ConcurrentHashMap<URI, Configuration> mapconf = new ConcurrentHashMap<>(2);
		ConcurrentHashMap<URI, UserGroupInformation> mapUgi = new ConcurrentHashMap<>(2);
		
		
		List<URI> list = Collections.synchronizedList(new ArrayList<>());

		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:\\hadoop-conf\\167\\hdfs.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		String confStr ="D:\\hadoop-conf\\167\\krb5.conf";
		System.setProperty("java.security.krb5.conf", confStr);
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation ugi1 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab);
		//printFile(dir + "user");
		list.add(new URI(dir));
		ugi1.doAs(new PrivilegedAction<FileSystem>() {
			@Override
			public FileSystem run() {
				FileSystem fs = null;
				try {
					fs = FileSystem.get(new URI(dir), conf);
					System.out.println(fs.exists(new Path(dir)));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
				return fs;
				
			}
			
		});
		
		
		
		String user1 = "hdfs/master@HADOOP.COM";
		String keytab1 = "D:\\hadoop-conf\\18\\hdfs.keytab";
		String dir1 = "hdfs://192.168.200.18:8020/";
		String confStr1 = "D:\\hadoop-conf\\18\\krb5.conf";
		list.add(new URI(dir1));
		Configuration conf2 = new Configuration();
		conf2.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf2);
		System.setProperty("java.security.krb5.conf", confStr1);
		UserGroupInformation ugi2 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1, keytab1);
		//printFile(dir1 + "user");
		
		mapconf.put(new URI(dir), conf);
		mapconf.put(new URI(dir1), conf2);
		
		mapUgi.put(new URI(dir), ugi1);
		mapUgi.put(new URI(dir1), ugi2);
		
		
		
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		Random r = new Random();
		// int nextInt = 1;
		for (int i = 0; i < 10000; i++) {
			fixedThreadPool.submit(() -> {
				String string2 = "";
				String dd = "";
					int nextInt = 0;
					synchronized (r) {
						nextInt = r.nextInt(2);
						System.out.println(nextInt);
					}
					URI string = list.get(0);
					UserGroupInformation subject = mapUgi.get(string);
					subject.doAs(new PrivilegedAction<FileSystem>() {
						@Override
						public FileSystem run() {
							FileSystem fs = null;
							try {
								Configuration configuration = mapconf.get(string);
								configuration.set("hadoop.security.authentication", "kerberos");
								fs = FileSystem.get(new URI(dir1), configuration);
								System.out.println(string+"------"+fs.exists(new Path(string)));
							} catch (IOException e) {
								e.printStackTrace();
							} catch (URISyntaxException e) {
								e.printStackTrace();
							}
							return fs;
							
						}
						
					});
			});
		}
	}

	public static Subject login1(String user, String keytab, String confStr) throws LoginException, IOException {
		// Configuration conf = new Configuration();
		// conf.set("hadoop.security.authentication", "kerberos");
		System.setProperty("java.security.krb5.conf", confStr);
		// UserGroupInformation.setConfiguration(conf);
		KerberosUtil kb = new KerberosUtil();
		LoginContext loginContextFromKeytab = kb.getLoginContextFromKeytab(user, keytab);
		loginContextFromKeytab.login();
		Subject subject = loginContextFromKeytab.getSubject();
		// UserGroupInformation.loginUserFromSubject(subject);
		return subject;
	}

	public static void printFile(String dir) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		FileSystem fs = FileSystem.get(new URI(dir), conf);
		// System.out.println("====================="+Thread.currentThread().getName());
		// System.out.println("READING ============================");
		Path path = new Path(dir);
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			//System.out.println(Thread.currentThread().getName() + " 000000000000000000000 " + fileStatus.getPath());
		}
	}

	public static void printFile(String dir, Configuration conf) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(dir), conf);
		// System.out.println("====================="+Thread.currentThread().getName());
		// System.out.println("READING ============================");
		Path path = new Path(dir);
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus fileStatus : files) {
			//System.out.println(Thread.currentThread().getName() + " 000000000000000000000 " + fileStatus.getPath());
		}
	}

}
