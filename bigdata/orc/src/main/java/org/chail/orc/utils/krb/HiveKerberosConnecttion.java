package org.chail.orc.utils.krb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.chail.orc.utils.BaseHdfsUtils;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;

/**
 * @ClassName : HiveKerberosConnecttion
 * @Description : hive kerberos登陆
 * @Author : Chail
 * @Date: 2020-11-02 20:04
 */
public class HiveKerberosConnecttion extends BaseHdfsUtils {


    public HiveKerberosConnecttion(String path, Configuration configuration) throws Exception {
        super(path, configuration);
    }

    public HiveKerberosConnecttion() throws Exception {
    }

    /**
     * 初始化
     *
     * @param path
     * @throws Exception
     */
    public HiveKerberosConnecttion(String path) throws Exception {
        super(path);
    }

    /**
     * 获取hive的连接
     *
     * @param databaseMeta
     * @return
     * @throws KettleDatabaseException
     * @throws Exception
     */
    public Connection getHiveKerberosConnecttion(DatabaseMeta databaseMeta) throws KettleDatabaseException, Exception {
        boolean kerberos = "true".equalsIgnoreCase(
            databaseMeta.getAttributes().getProperty(KrbConstant.DM_HIVE_KERBEROS_ENABLE)) ? true : false;
        // 无kerberos
        Connection connection = null;
        if (kerberos) {
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "kerberos");
            String principal = databaseMeta.getAttributes().getProperty(KrbConstant.KEY_DM_KERBEROS_PRINCIPAL);
            String keytab = databaseMeta.getAttributes().getProperty(KrbConstant.KEY_DM_KERBEROS_KEYTAB);
            String confStr = databaseMeta.getAttributes().getProperty(KrbConstant.KEY_DM_KERBEROS_KRB5_CONF);
            try {
                UserGroupInformation userGroupInformation = KerberosLoginSubject.loginFromSubject(databaseMeta.getURL(), principal, keytab, confStr);
                connection = userGroupInformation.doAs(new PrivilegedExceptionAction<Connection>() {
                    @Override
                    public Connection run() throws Exception {
                        UserGroupInformation.setConfiguration(configuration);
                        return getConnection(databaseMeta);
                    }
                });
            } catch (Exception e) {
                KerberosLoginSubject.removeErrorSubject(databaseMeta.getURL());
                LOGGER.error("验证失败===hive-db-" + principal, e);
            }
        } else {
            // 设置hive的用户为登录用户
            return getConnection(databaseMeta);
        }
        return connection;
    }


    private Connection getConnection(DatabaseMeta databaseMeta) throws KettleDatabaseException {
        Database database = new Database(null, databaseMeta);
        database.connect();
        return database.getConnection();
    }

}
