package org.chail.common.krb;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName : KerberosSubject
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-02 11:00
 */
public class KerberosLoginSubject {
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosLoginSubject.class);

    /**
     * kerberos登录的上下文
     */
    private static ConcurrentHashMap<String, MySubject> subjectCashMap = new ConcurrentHashMap<>();


    /**
     * 清楚缓存
     * @param filePath
     */
    public synchronized static void removeErrorSubject(String filePath){
        String authority = getAuthority(filePath);
        if(subjectCashMap.containsKey(authority)){
            subjectCashMap.remove(authority);
            LOGGER.debug("成功删除SUBJECT=>"+authority);
        }
    }

    /**
     * 登录UGI
     *
     * @param filePath
     * @param principal
     * @param keytab
     * @param krb5conf
     * @return
     * @throws LoginException
     */
    public synchronized static UserGroupInformation loginFromSubject(String filePath, String principal, String keytab,
                                                                     String krb5conf) throws LoginException, IOException {
        String authority = getAuthority(filePath);
        MySubject mySubject = subjectCashMap.get(authority);
        //kerberos
        if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(principal)) {
            checkPrincipal(keytab, principal);
        } else {
            //simpal
            mySubject = new MySubject(filePath);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(getAuthorityUser(filePath), SaslRpcServer.AuthMethod.SIMPLE);
            mySubject.setUserGroupInformation(ugi);
            return ugi;
        }
        boolean loginEnable = false;
        if (mySubject != null) {
            loginEnable = mySubject.checkTimeout();
        } else {
            loginEnable = true;
        }
        if (loginEnable) {
            System.setProperty(KrbConstant.JVM_KERBEROS_KRB5_CONF, krb5conf);
            mySubject = new MySubject(filePath);
            LoginContext login = newLoginContext(MyHadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME, mySubject.getSubject(), new MyHadoopConfiguration(authority,keytab, principal));
            login.login();
            UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(mySubject.getSubject());
            mySubject.setUserGroupInformation(ugi);
            subjectCashMap.put(authority, mySubject);
        }
        return mySubject.getUserGroupInformation();
    }

    /**
     * 获取获取LoginContext
     *
     * @param appName
     * @param subject
     * @param loginConf
     * @return
     * @throws LoginException
     */
    private static LoginContext newLoginContext(String appName, Subject subject, javax.security.auth.login.Configuration loginConf) throws LoginException {
        Thread t = Thread.currentThread();
        ClassLoader oldCCL = t.getContextClassLoader();
        t.setContextClassLoader(MyHadoopLoginModule.class.getClassLoader());
        LoginContext var5;
        try {
            var5 = new LoginContext(appName, subject, new MyCallbackHandler(), loginConf);
        } finally {
            t.setContextClassLoader(oldCCL);
        }

        return var5;
    }


    /**
     * 监测principal
     *
     * @param keytabFileName
     * @return
     * @throws IOException
     */
    public static final String[] getPrincipalNames(String keytabFileName) throws IOException {
        Keytab keytab = Keytab.read(new File(keytabFileName));
        Set<String> principals = new HashSet<String>();
        List<KeytabEntry> entries = keytab.getEntries();
        for (KeytabEntry entry : entries) {
            principals.add(entry.getPrincipalName().replace("\\", "/"));
        }
        return principals.toArray(new String[0]);
    }


    public static boolean checkPrincipal(String keytabFileName, String principal) throws IOException {
        String[] principalNames = getPrincipalNames(keytabFileName);
        StringBuffer sbf = new StringBuffer();
        if (principalNames != null && principalNames.length > 0) {
            for (String str : principalNames) {
                if (str.equals(principal)) {
                    return true;
                }
                if (sbf.length() > 0) {
                    sbf.append(",");
                    sbf.append("[");
                    sbf.append(str);
                    sbf.append("]");
                } else {
                    sbf.append("[");
                    sbf.append(str);
                    sbf.append("]");
                }
            }
        }
        throw new IOException("principal :" + principal + "错误,查询到keytab的princial为:" + sbf.toString());
    }

    public static String getAuthority(String filePath) {
        try {
            URI uri = new URI(filePath);
            String authority = uri.getAuthority();
            if (StringUtils.isEmpty(authority)) {
                return filePath;
            }
            return uri.getAuthority();
        } catch (Exception e) {
            LOGGER.debug("获取权限错误", e);
            return filePath;
        }
    }

    /**
     * 获取url名字
     *
     * @param filePath
     * @return
     */
    public static String getAuthorityUser(String filePath) {
        try {
            URI uri = new URI(filePath);
            String userInfo = uri.getUserInfo();
            if (StringUtils.isEmpty(userInfo)) {
                return System.getProperty(KrbConstant.HADOOP_USER_NAME, "hive");
            }
            return userInfo;
        } catch (Exception e) {
            LOGGER.debug("获取权限错误", e);
            return System.getProperty(KrbConstant.HADOOP_USER_NAME, "hive");
        }
    }


}
