package org.chail;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

//
// 服务端节点
public class ServiceNode {
	public static void main(String[] args) {
		// 获取Hazelcast实例
		HazelcastInstance ins = Hazelcast.newHazelcastInstance();

		// 从集群中读取Map实例
		Map<Integer, String> map = ins.getMap("default map");

		// 向集群中添加数据
		System.out.println("Begin insert data");
		map.put(1, "Cait Cassiopeia");
		map.put(2, "Annie");
		map.put(3, "Evelynn");
		map.put(4, "Ashe");
		System.out.println("End");
		
	}
}