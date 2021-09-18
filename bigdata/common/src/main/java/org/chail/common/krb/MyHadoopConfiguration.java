package org.chail.common.krb;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.util.PlatformName;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class MyHadoopConfiguration extends javax.security.auth.login.Configuration {
    public static final String SIMPLE_CONFIG_NAME = "hadoop-simple";
    public static final String USER_KERBEROS_CONFIG_NAME = "hadoop-user-kerberos";
    public static final String KEYTAB_KERBEROS_CONFIG_NAME = "hadoop-keytab-kerberos";
    private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap();
    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN;
    private static final AppConfigurationEntry HADOOP_LOGIN;
    private static final Map<String, String> USER_KERBEROS_OPTIONS;
    private static final AppConfigurationEntry USER_KERBEROS_LOGIN;
    private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS;
    private static final boolean windows = System.getProperty("os.name").startsWith("Windows");
    private static final boolean is64Bit = System.getProperty("os.arch").contains("64");
    private static final boolean aix = System.getProperty("os.name").equals("AIX");
    private static String OS_LOGIN_MODULE_NAME = getOSLoginModuleName();

    public  static final  String MCHZ_AUTH_KEY="mchz.auth.key";


    private String keytabFile;
    private  String keytabPrincipal;
    private  String authKey;

    public MyHadoopConfiguration() {
    }

    public  MyHadoopConfiguration(String authKey,String keytabFile, String keytabPrincipal) {
        this.keytabFile=keytabFile;
        this.keytabPrincipal=keytabPrincipal;
        this.authKey=authKey;
    }

    /**
     * Retrieve the AppConfigurationEntries for the specified <i>name</i>
     * from this Configuration.
     *
     * <p>
     *
     * @param appName the name used to index the Configuration.
     * @return an array of AppConfigurationEntries for the specified <i>name</i>
     * from this Configuration, or null if there are no entries
     * for the specified <i>name</i>
     */
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        KerberosName.setRules("DEFAULT");
        if ("hadoop-simple".equals(appName)) {
            return new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, HADOOP_LOGIN};
        } else if ("hadoop-user-kerberos".equals(appName)) {
            return new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN, HADOOP_LOGIN};
        } else if ("hadoop-keytab-kerberos".equals(appName)) {
            if (PlatformName.IBM_JAVA) {
                KEYTAB_KERBEROS_OPTIONS.put("useKeytab", prependFileAuthority(keytabFile));
            } else {
                KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                if(StringUtils.isNotEmpty(authKey)){
                    USER_KERBEROS_OPTIONS.put(MCHZ_AUTH_KEY, authKey);
                }
            }
            KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
            return new AppConfigurationEntry[]{new AppConfigurationEntry(org.apache.hadoop.security.
                authentication.util.KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.
                REQUIRED, KEYTAB_KERBEROS_OPTIONS), HADOOP_LOGIN};
        } else {
            return null;
        }
    }





    static {
        String ticketCache = System.getenv("HADOOP_JAAS_DEBUG");
        if (ticketCache != null && "true".equalsIgnoreCase(ticketCache)) {
            BASIC_JAAS_OPTIONS.put("debug", "true");
        }
        OS_SPECIFIC_LOGIN = new AppConfigurationEntry(OS_LOGIN_MODULE_NAME, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);
        HADOOP_LOGIN = new AppConfigurationEntry(MyHadoopLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);
        USER_KERBEROS_OPTIONS = new HashMap();
        USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
        ticketCache = System.getenv("KRB5CCNAME");
        if (ticketCache != null) {
            USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
        }
        USER_KERBEROS_OPTIONS.put("renewTGT", "true");
        USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        USER_KERBEROS_LOGIN = new AppConfigurationEntry(org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS);
        KEYTAB_KERBEROS_OPTIONS = new HashMap();
        KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
        KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");

        KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
        KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);

    }


    private static String getOSLoginModuleName() {
        if (PlatformName.IBM_JAVA) {
            if (windows) {
                return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule" : "com.ibm.security.auth.module.NTLoginModule";
            } else if (aix) {
                return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule" : "com.ibm.security.auth.module.AIXLoginModule";
            } else {
                return "com.ibm.security.auth.module.LinuxLoginModule";
            }
        } else {
            return windows ? "com.sun.security.auth.module.NTLoginModule" : "com.sun.security.auth.module.UnixLoginModule";
        }
    }

    private static String prependFileAuthority(String keytabPath) {
        return keytabPath.startsWith("file://") ? keytabPath : "file://" + keytabPath;
    }


    public static final String[] getPrincipalNames(String keytabFileName) throws IOException {
        Keytab keytab = Keytab.read(new File(keytabFileName));
        Set<String> principals = new HashSet<String>();
        List<KeytabEntry> entries = keytab.getEntries();
        for (KeytabEntry entry : entries) {
            principals.add(entry.getPrincipalName().replace("\\", "/"));
        }
        return principals.toArray(new String[0]);
    }


    public String getKeytabFile() {
        return keytabFile;
    }

    public void setKeytabFile(String keytabFile) {
        this.keytabFile = keytabFile;
    }

    public String getKeytabPrincipal() {
        return keytabPrincipal;
    }

    public void setKeytabPrincipal(String keytabPrincipal) {
        this.keytabPrincipal = keytabPrincipal;
    }

}
