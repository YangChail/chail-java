package com.mchz.ds.dsync;

import com.mchz.discovery.client.DefaultDiscoveryClient;
import com.mchz.discovery.client.constant.ServingModeEnum;

public class ZkRegister {
    public static void registry(){
        String zk = System.getProperty("zk.address");
        String dsync = System.getProperty("dsync.address");
        if(zk==null||dsync==null||zk.length()<1||dsync.length()<1){
            return ;
        }
        new DefaultDiscoveryClient(zk,"dsync",dsync, ServingModeEnum.CLUSTER).startup();
    }
}
