package com.chail.apputil.jdbc.hive;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;
import com.chail.apputil.jdbc.jdbcutilsone.JdbcDirver;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HiveJdbc {

	public static void testHiveKb() throws Exception {
		String userkb = "hive/slave1@CDH167.COM";
		String keytabkb = "D:/hive.keytab";
		Configuration conf = new Configuration();
		// conf.addResource(new Path("D:/hdfs-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		// conf.addResource(new Path("D:/core-site.xml"));
		System.setProperty("java.security.krb5.conf", "D:/krb5167.conf");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(userkb, keytabkb);
		//UserGroupInformation.setConfiguration(conf);
		//System.setProperty("java.security.krb5.conf", "D:/krb5.ini");
		String user = "hive";
		String pass = "hive";
		//String url = "jdbc:hive2://192.168.200.18:10000/default;principal=hive/master@HADOOP.COM";
		String url = "jdbc:hive2://192.168.200.167:10000/default;principal=hive/slave1@CDH167.COM";
		
		JDBCUtil jdbcUtil = new JDBCUtil(url, JdbcDirver.HIVE_DRIVER, user, pass);
		jdbcUtil.getConnection();
		String sql = "show tables";
		List<Map<String, Object>> executeQuery = jdbcUtil.executeQuery(sql);
		System.out.println(executeQuery.size());
		jdbcUtil.releaseConnectn();
	}
	
	
	private static void copytab() throws Exception {
		String sql="create table chail.tb_?  as select * from chail.tb_0000";
		List<String> list=new ArrayList<String>();
		Random random = new Random();
		for(int i=6;i<10000;i++) {
			int nextInt = random.nextInt(5)+1;
			String sqlaa=String.format("%05d", i);
			String ss=sql.replace("?", sqlaa)+nextInt;
			list.add(ss);
			
			
		
			//System.out.println(ss);
		}
	    LinkedBlockingQueue<Runnable> queue= new LinkedBlockingQueue<Runnable>();
	    ExecutorService pool =getPoll(5,queue);
		for (String sqls : list) {
			pool.execute(() -> {
				System.out.println(sqls);
				JDBCUtil jdbcUtil = getJdbc();
				jdbcUtil.getConnection();
				jdbcUtil.executeUpdate(sqls);
				//
				jdbcUtil.releaseConnectn();
			});
		}
		
		waitPoll(pool,queue);
	}
	
	
	
	
	public static void insertPartiton() throws InterruptedException {
		LinkedBlockingQueue<Runnable> queue= new LinkedBlockingQueue<Runnable>();
	    ExecutorService pool =getPoll(30,queue);
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		String sql="insert into  many_partition.big_part partition(dt='?') select * from chail.tb_";
		List<String> list=new ArrayList<String>();
		String end=" limit 100 ";
		Random random = new Random();
		c .add(Calendar.DATE,-500);
		for(int i=1;i<1000;i++) {
			int ii=random.nextInt(25)+1;
			String sqlaa=String.format("%05d", ii);
			c .add(Calendar.DATE, 1);
			String date=sdf.format(c.getTime());
				list.add(sql.replace("?", date)+sqlaa+end);
			
			
		}
		BlockingQueue<JDBCUtil> conPoll = getConPoll(30);
		
		for (String sqls : list) {
			pool.execute(() -> {
				System.out.println(sqls);
				JDBCUtil jdbcUtil = null;
				try {
					jdbcUtil = conPoll.take();
					jdbcUtil.executeUpdate(sqls);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}finally {
					try {
						conPoll.put(jdbcUtil);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			});
		}
		
	}
	
	public static JDBCUtil getJdbc() {
		String user = "hive";
		String pass = "hive";
		//String url = "jdbc:hive2://192.168.241.104:10000/default";
		String url = "jdbc:hive2://192.168.239.1:10000/default";
		return new JDBCUtil(url, JdbcDirver.HIVE_DRIVER, user, pass);
	}
	
	public static void waitPoll(ExecutorService pool,LinkedBlockingQueue<Runnable> queue) {
		try {
			boolean loop = true;
			do { // 等待所有任务完成
					// 阻塞，直到线程池里所有任务结束
				loop = !pool.awaitTermination(10, TimeUnit.SECONDS);
				int size = queue.size();
				System.out.println(size);
			} while (loop);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static ExecutorService getPoll(int threadsize,LinkedBlockingQueue<Runnable> queue) {
		    ExecutorService pool = new  ThreadPoolExecutor(threadsize, threadsize,
	                20L, TimeUnit.SECONDS,
	                queue,new ThreadFactoryBuilder()
		            .setNameFormat("dm-hive-%d").build());
		    
		    return pool;
		    
	}
	
	
	public static BlockingQueue<JDBCUtil> getConPoll(int size) throws InterruptedException {
		 BlockingQueue<JDBCUtil> basket = new ArrayBlockingQueue<JDBCUtil>(size);
		 for(int i=0;i<size;i++) {
			 JDBCUtil jdbc = getJdbc();
			 jdbc.getConnection();
			 basket.put(jdbc);
		 }
		return basket;
	}
	
	
	
	public static void main(String[] args) throws Exception {
		testHiveKb();
		//copytab();
		//insertPartiton();
	}
}
