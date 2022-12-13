package app.chail;

import com.mchz.discovery.client.DefaultDiscoveryClient;
import com.mchz.discovery.client.constant.ServingModeEnum;
import com.mchz.discovery.client.model.Service;
import com.mchz.discovery.client.model.Worker;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

public class KakfaTests1 {
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String RESOURCES_FILEPATH = System.getProperty("user.dir") + File.separator + "conf";
    public static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";
    private static Properties CONFIG = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(KakfaTests1.class);

    public static void main(String[] args) throws IOException {
        logger.info("我是info信息");    //info级别的信息
        registry();
        listTopic();

    }


    private static void registry() {
        DefaultDiscoveryClient defaultDiscoveryClient = new DefaultDiscoveryClient("127.0.0.1:2181","127.0.0.1:2181", "test-aa", "192.169.123.1:8081", ServingModeEnum.CLUSTER);
        defaultDiscoveryClient.startup();
        CuratorFramework framework = defaultDiscoveryClient.getFramework();
        CuratorZookeeperClient zookeeperClient = framework.getZookeeperClient();
        new Thread(new Runnable() {
            @Override
            public void run() {
                int i = 0;
                while (true) {
                    //Service service = defaultDiscoveryClient.getService("test-aa");
                    // logger.info("注册service-----"+service.getName());
                    // logger.info("注册service-----"+service.getWorkers().get(0).getAddress());
                    Object o = null;
                    try {
                        o = RetryLoop.callWithRetry(zookeeperClient, new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                                int allChildrenNumber = 0;
                                if (zookeeperClient.isConnected()) {
                                    zookeeperClient.close();
                                    //System.getProperties().remove("java.security.auth.login.config");
                                     logger.info("xxxxxxxxxxxxxxxxx=========start===========xxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                                    //                                            Configuration configuration = Configuration.getConfiguration();
                                    //                                            configuration.refresh();
                                    try {
                                        zookeeperClient.start();
                                        zookeeperClient.getZooKeeper().getClientConfig().setProperty("zookeeper.sasl.client", "false");
                                        allChildrenNumber = zookeeperClient.getZooKeeper().getAllChildrenNumber("/");
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    allChildrenNumber = zookeeperClient.getZooKeeper().getAllChildrenNumber("/");
                                }
                                return allChildrenNumber;
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                     logger.info("00000000000000000000000-zk 根节点：{}-0000000000000000000000000000000" , o);
                    sleep(5000);
                    i++;

                }
            }
        }).start();
        sleep(4000);


    }

    private static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void readConf() throws IOException {
        CONFIG.load(new FileInputStream(RESOURCES_FILEPATH + File.separator + "config1.properties"));
    }

    private static void listTopic() throws IOException {
        readConf();
        securityPrepare();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //System.setProperty("java.security.auth.login.config", tempFile);
                        Configuration configuration = Configuration.getConfiguration();
                        configuration.refresh();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Map<String, List<PartitionInfo>> stringListMap = new KafkaConsumer<>(getProperty()).listTopics();
//                    stringListMap.forEach((k, v) -> {
//                         logger.info(k);
//                    });
                    logger.info("==================================Kerberos kafka topic size:{}==============================================================",stringListMap.size());
                    sleep(5000);
                }
            }
        });
        thread.start();

        while (thread.isAlive()) {
            sleep(10000);
        }

    }


    public static <T> T doAs(PrivilegedAction<T> action) {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);
        return Subject.doAs(subject, action);
    }


    /**
     * 安全认证
     */
    private static void securityPrepare() throws IOException {
        //System.setProperty("sun.security.krb5.debug","true");
        System.setProperty("zookeeper.server.principal", CONFIG.getProperty("zookeeper.server.principal"));
        System.setProperty(JAVA_SECURITY_KRB5_CONF, RESOURCES_FILEPATH + File.separator + "krb51.conf");
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, RESOURCES_FILEPATH + File.separator + "user1.jaas");
    }


    private static Properties getProperty() {
        Properties clientProps = new Properties();
        clientProps.put("kerberos.domain.name", CONFIG.getProperty("kerberos.domain.name"));
        clientProps.put("application.id", UUID.randomUUID().toString());
        clientProps.put("security.protocol", "SASL_PLAINTEXT");
        clientProps.put("auto.offset.reset", "earliest");
        clientProps.put("group.id", UUID.randomUUID().toString());
        clientProps.put("sasl.kerberos.service.name", "kafka");
        clientProps.put("bootstrap.servers", CONFIG.getProperty("bootstrap.servers"));
        clientProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return clientProps;
    }

}
