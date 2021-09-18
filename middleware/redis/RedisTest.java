package com.chail.apputil.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.omg.PortableServer.THREAD_POLICY_ID;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;

public class RedisTest {

	public static JedisPool initialPool(int size, String host, int prot, String passwd, String database)
			throws Exception {
		// 池基本配置
		try {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(size);
			config.setMaxWaitMillis(10000l);
			config.setTestOnBorrow(true);  
			if (StringUtils.isEmpty(passwd)) {
				return new JedisPool(config, host, prot);
			}
			int db = Protocol.DEFAULT_DATABASE;
			if (StringUtils.isNotEmpty(database) && StringUtils.isNumeric(database)) {
				db = Integer.valueOf(database);
			}
			return new JedisPool(config, host, prot, 60000, passwd, db);
		} catch (Exception e) {
			throw e;
		}

	}

	public static void main(String[] args) throws Exception {
		//get2();
		set();
	}

	public static void get2() throws Exception {
		JedisPool initialPool = RedisTest.initialPool(5, "192.168.241.106", 6379, "foobared","0");
		Jedis resource = initialPool.getResource();
		Set<String> keys = resource.keys("*");
		System.out.println(keys.size());
		Pipeline pipelined = resource.pipelined();
		Jedis resource2 = initialPool.getResource();
		Pipeline pipelined2 = resource2.pipelined();
		Iterator<String> iterator = keys.iterator();
		Iterator<String> iterator2 = keys.iterator();
		long start = System.currentTimeMillis();
		List<Response<String>> list=new ArrayList<Response<String>>();
		List<Response<String>> list2=new ArrayList<Response<String>>();
		for(int i=0;i<keys.size();i++) {
			String next = iterator.next();
			list.add(pipelined.get(next)) ;
		}
		
		for(int i=0;i<keys.size();i++) {
			String next = iterator2.next();
			list2.add(pipelined2.type(next)) ;
		}
		pipelined.sync(); 
		pipelined2.sync(); 
		long end = System.currentTimeMillis();
		for(int i=0;i<list.size();i++) {
			String string = list.get(i).get();
			String string2 = list2.get(i).get();
			
		}
		System.out.println(end-start);
		System.out.println(list.size());
		
	}
	
	
	 class Value {

		 String key;
		 String value;
		 String type;
		
		
	}
	
	
	
	public static void get() throws Exception {
		JedisPool initialPool = RedisTest.initialPool(5, "192.168.241.106", 6379, "foobared","0");
		Jedis resource = initialPool.getResource();
		Set<String> keys = resource.keys("*");
		System.out.println("222");
		LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(10, 10, 20L, TimeUnit.SECONDS, queue,
				new ThreadFactoryBuilder().setNameFormat("dm-builder-%d").build());
		for (String str : keys) {
			pool.execute(() -> {
				//System.out.println("sdsdsdsd");
				Jedis jedis = initialPool.getResource();
				String string = jedis.get(str);
				//System.out.println(str + "===>" + string);
				jedis.close();
			});
		}
		try {
			boolean loop = true;
			do { // 等待所有任务完成
					// 阻塞，直到线程池里所有任务结束
				int queueSize = pool.getQueue().size();
				System.out.println("当前排队线程数：" + queueSize);
				int activeCount = pool.getActiveCount();
				System.out.println("当前活动线程数：" + activeCount);
				long completedTaskCount = pool.getCompletedTaskCount();
				System.out.println("执行完成线程数：" + completedTaskCount);
				long taskCount = pool.getTaskCount();
				System.out.println("总线程数：" + taskCount);
				loop = !pool.awaitTermination(1, TimeUnit.SECONDS);
			} while (loop);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("ddddddddddddddddddddd");

	}

	public static void set() throws Exception {
		JedisPool initialPool = RedisTest.initialPool(10, "192.168.241.106", 6379, "foobared","0");
		Jedis resource = initialPool.getResource();
		Pipeline pipelined = resource.pipelined();
		for (int i = 1; i < 10000000; i++) {
			String key = "dm-" + System.currentTimeMillis();
			 resource.set(key, "我是第一帅");
			if(i%10000==0) {
				System.out.println(i);
				Thread.sleep(1000);
			}
		}
		initialPool.destroy();
	}

}
