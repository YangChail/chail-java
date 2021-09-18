package org.chail.orc.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.chail.orc.utils.krb.KrbConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName : KrbVerifile
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-02 19:35
 */
public class HadoopConstant {


    public static final String CORE_SITE_FILE_NAME = "core-site.xml";
    public static final String HDFS_SITE_NAME = "hdfs-site.xml";
    public static final String YARN_SITE_NAME = "yarn-site.xml";
    public static final String MAPRED_SITE_NAME = "mapred-site.xml";
    public static final String KRB5_FILE_NAME = "krb5.conf";
    public static final String HBASE_SITE_FILE_NAME = "hbase-site.xml";


    public static final String[] CONFIG_ARRAY = { CORE_SITE_FILE_NAME, HDFS_SITE_NAME, YARN_SITE_NAME,
        MAPRED_SITE_NAME };

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopConstant.class);
    /**
     * 配置缓存
     */
    public static ConcurrentHashMap<String, Map<String, String>> CONFIG_AND_SUBJECT_MAP = new ConcurrentHashMap<>();


    public static Configuration setConfig(String path,Configuration configuration) throws Exception {
        URI url = new URI(path);
        String authority = url.getAuthority();
        if(configuration==null) {
            configuration = new Configuration();
        }
        if(authority==null) {
            return configuration;
        }
        Map<String, String> map = CONFIG_AND_SUBJECT_MAP.get(authority);
        if (map == null || map.size() < 1) {
            LOGGER.error("配置文件为空,文件路径为 "+path);
            return configuration;
            //throw new IOException("配置文件为空,文件路径为 "+path);
        }
        HadoopConstant.addConfig(map, configuration);
        if ( KrbConstant.isKerberosEnbale(configuration)) {
            KrbConstant.setKerberosSubjectInfo(configuration, map);
        }
        setCustomerConfig(configuration);
        return configuration;
    }


    public static void setCustomerConfig(Configuration configuration){
        configuration.set("fs.hdfs.impl.disable.cache", "true");
        configuration.set("ipc.client.rpc-timeout.ms", "100000");
        configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true");


    }



    public static void addConfig(Map<String, String> kerberosSubjectMap, Configuration conf) {
        for (String str : CONFIG_ARRAY) {
            String string = kerberosSubjectMap.get(str);
            if (StringUtils.isNotEmpty(string)&&conf.toString().indexOf(string) < 0) {
                Path uri = new Path(string);
                //LOGGER.info("config ..{}",string);
                conf.addResource(uri);
            }
        }
    }


}
