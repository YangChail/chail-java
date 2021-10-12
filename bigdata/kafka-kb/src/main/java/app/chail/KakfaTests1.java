package app.chail;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class KakfaTests1 {
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String RESOURCES_FILEPATH = System.getProperty("user.dir") + File.separator + "conf" ;
    public static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";
    private static Properties CONFIG = new Properties();
    private static final Logger LOG = LoggerFactory.getLogger(KakfaTests1.class);

    public static void main(String[] args) throws IOException {

        LOG.info("sss");
        readConf();
        securityPrepare();
        listTopic(getProperty());
    }


    private static void readConf() throws IOException {
        CONFIG.load(new FileInputStream(RESOURCES_FILEPATH +  File.separator+"config1.properties"));
    }

    private static void listTopic(Properties clientProps) {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(clientProps);
        Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        stringListMap.forEach((k,v)->{
            System.out.println(k);
        });
    }


    /**
     * 安全认证
     */
    private static void securityPrepare() throws IOException {
        //System.setProperty("sun.security.krb5.debug","true");
        System.setProperty("zookeeper.server.principal", CONFIG.getProperty("zookeeper.server.principal"));
        System.setProperty(JAVA_SECURITY_KRB5_CONF, RESOURCES_FILEPATH + File.separator+ "krb51.conf");
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, RESOURCES_FILEPATH +  File.separator+"user1.jaas");
    }


    private static Properties getProperty(){
        Properties clientProps = new Properties();
        clientProps.put("kerberos.domain.name", CONFIG.getProperty("kerberos.domain.name"));
        clientProps.put("application.id", UUID.randomUUID().toString());
        clientProps.put("security.protocol","SASL_PLAINTEXT");
        clientProps.put("auto.offset.reset","earliest");
        clientProps.put("group.id", UUID.randomUUID().toString());
        clientProps.put("sasl.kerberos.service.name","kafka");
        clientProps.put("bootstrap.servers", CONFIG.getProperty("bootstrap.servers"));
        clientProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return clientProps;
    }

}
