package org.chail.orc.utils.krb;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * @ClassName : KrbVerifile
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-02 19:35
 */
public class KrbConstant {

    public static final String  HADOOP_USER_NAME = "HADOOP_USER_NAME";
    public static final String JVM_KERBEROS_KRB5_CONF = "java.security.krb5.conf";
    public static final String KEY_DM_KERBEROS_PRINCIPAL = "dm.kerberos.principal";
    public static final String KEY_DM_KERBEROS_KRB5_CONF = "dm.kerberos.krb5.conf";
    public static final String KEY_DM_KERBEROS_KEYTAB = "dm.kerberos.keytab";
    public static final String DM_HIVE_KERBEROS_ENABLE = "dm.hive.kerberos.enable";
    public static final String DM_HIVE_KERBEROS_PRINCIPAL = "dm.hive.kerberos.principal";
    public static final String[] CONFIG_ARRAY = {KEY_DM_KERBEROS_PRINCIPAL, KEY_DM_KERBEROS_KRB5_CONF,
        KEY_DM_KERBEROS_KEYTAB, DM_HIVE_KERBEROS_ENABLE};


    /**
     * 判断是否是kerberos
     * @param configuration
     * @return
     */
    public static boolean isKerberosEnbale(Configuration configuration) {
        return configuration.get("hadoop.security.authentication").equalsIgnoreCase("kerberos") ? true : false;
    }

    /**
     * 设置kerboers是否有
     *
     * @param conf
     * @param kerberosSubjectMap
     * @throws Exception
     */
    public static void setKerberosSubjectInfo(Configuration conf, Map<String, String> kerberosSubjectMap)
        throws Exception {
        if (kerberosSubjectMap != null && kerberosSubjectMap.size() > 0) {
            for (String str : CONFIG_ARRAY) {
                String value = kerberosSubjectMap.get(str);
                if (value == null) {
                    if (str.equalsIgnoreCase(DM_HIVE_KERBEROS_ENABLE)) {
                        conf.set(DM_HIVE_KERBEROS_ENABLE, "true");
                        continue;
                    }
                    throw new Exception("core-site.xml中开启了kerberos,请输入kerberos 相关配置");
                }
                conf.set(str, value);
            }
            // 设置非kerberos 的登录
            conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        }
    }



    public static String getKerberosPrincipal(Configuration conf) throws Exception {
        return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_PRINCIPAL);
    }

    public static String getKerberosKeytab(Configuration conf) throws Exception {
        return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_KEYTAB);
    }

    public static String getKerberoskrb5(Configuration conf) throws Exception {
        return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_KRB5_CONF);
    }


    public static String getKerberosConfigFormHadoopConfiguration(Configuration conf, String key) throws Exception {
        String confStr = conf.get(key);
        if (StringUtils.isEmpty(confStr)) {
            throw new Exception("配置文件缺少配置 " + key);
        } else {
            return confStr;
        }
    }


}
