package com.mchz.bigdata.hbase;

import com.mchz.bigdata.hdfs.utils.HdfsUtil;
import com.mchz.bigdata.hdfs.utils.KerboersUtils;
import com.mchz.bigdata.hdfs.utils.LoginUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Properties;

public class HBaseConn {
    private Configuration configuration; // hbase配置
    private Connection connection; // hbase connection
    private String zkHostAndPort;
    private boolean iskerberos;
    private String principal;
    private String keytab;
    private String confStr;

    public HBaseConn(Configuration configuration, Connection connection, String zkHostAndPort) {
        super();
        this.configuration = configuration;
        this.connection = connection;
        this.zkHostAndPort = zkHostAndPort;
    }

    /*
	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		super.init(smi, sdi);
		HbaseInputData data = (HbaseInputData) sdi;
		HbaseInputMeta meta = (HbaseInputMeta) smi;
		String zookeeperHost = meta.getZookeeperHost();
		data.hbaseTable = meta.getHbaseTable();

		Properties properties=new Properties();
		if(StringUtils.isNotEmpty(meta.getHbaseconfig())) {
			properties.put(HdfsUtil.HBASE_SITE_FILE_NAME, meta.getHbaseconfig());
		}
	
		if(StringUtils.isNotEmpty(meta.getCoreConfig())) {
			properties.put(HdfsUtil.CORE_SITE_FILE_NAME, meta.getCoreConfig());
		}
		if( meta.isKerberosEnbale()) {
			properties.put(	KerboersUtils.DM_HIVE_KERBEROS_ENABLE, "true");
			if(StringUtils.isNotEmpty(meta.getPrincipal())) {
				properties.put(KerboersUtils.KEY_DM_KERBEROS_PRINCIPAL, meta.getPrincipal());
			}
			if(StringUtils.isNotEmpty(meta.getKeytabPath())) {
				properties.put(KerboersUtils.KEY_DM_KERBEROS_KEYTAB, meta.getKeytabPath());
			}
			
			if(StringUtils.isNotEmpty(meta.getKrb5Config())) {
				properties.put(KerboersUtils.KEY_DM_KERBEROS_KRB5_CONF, meta.getKrb5Config());
			}
		}
		HbaseConn hbaseConn = new HbaseConn(zookeeperHost,properties);
		
		data.hBaseUtil = new HBaseUtil(hbaseConn);
		try {
			data.outRowMeta = HbaseValueUtil.getOutRowMeta(data.hbaseTable);
		} catch (KettlePluginException e) {
			return false;
		}
		return true;
		
	
	}  
	*/  
    
    public HBaseConn(String zkHostAndPort, String hbaseConfigPath, String coresiteConfigPath) {
        createConfig();
        configuration.set("hbase.zookeeper.quorum", zkHostAndPort);
    }

    public HBaseConn(String zkHostAndPort, Properties properties) {
        createConfig();
        if (StringUtils.isNotEmpty(properties.getProperty(HdfsUtil.CORE_SITE_FILE_NAME, ""))) {
            configuration.addResource(new Path(properties.getProperty(HdfsUtil.CORE_SITE_FILE_NAME, "")));
        }
        if (StringUtils.isNotEmpty(properties.getProperty(HdfsUtil.HDFS_SITE_NAME, ""))) {
            configuration.addResource(new Path(properties.getProperty(HdfsUtil.HDFS_SITE_NAME, "")));
        }
        if (StringUtils.isNotEmpty(properties.getProperty(HdfsUtil.HBASE_SITE_FILE_NAME, ""))) {
            configuration.addResource(new Path(properties.getProperty(HdfsUtil.HBASE_SITE_FILE_NAME, "")));
        }
        this.zkHostAndPort = zkHostAndPort;
        iskerberos = "true".equalsIgnoreCase(properties.getProperty(KerboersUtils.DM_HIVE_KERBEROS_ENABLE)) ? true
            : false;
        principal = properties.getProperty(KerboersUtils.KEY_DM_KERBEROS_PRINCIPAL, "");
        keytab = properties.getProperty(KerboersUtils.KEY_DM_KERBEROS_KEYTAB, "");
        confStr = properties.getProperty(KerboersUtils.KEY_DM_KERBEROS_KRB5_CONF, "");
        configuration.set("hbase.zookeeper.quorum", zkHostAndPort);
        configuration.set("hbase.client.retries.number", "10");
        configuration.set("hbase.client.scanner.timeout.period", "3000");
        if (iskerberos) {
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hbase.security.authentication", "kerberos");
        }
    }

    public HBaseConn(String zkHostAndPort) {
        createConfig();
        configuration.set("hbase.zookeeper.quorum", zkHostAndPort);
    }

    public void createConfig() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                if (iskerberos) {
                    String url = "HBASE_ZK_" + zkHostAndPort;
//                    UserGroupInformation loginUgi = KerboersUtils.getLoginUgi(url, principal, keytab, confStr,
//                        configuration);

                    LoginUtil.setZookeeperServerPrincipal(principal);
                    LoginUtil.login(principal,keytab,confStr,configuration);
                    connection = ConnectionFactory.createConnection(configuration);
                } else {
                    connection = ConnectionFactory.createConnection(configuration);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Connection getConnection(String principal, String keytab, String confStr) throws Exception {
        this.principal = principal;
        this.keytab = keytab;
        this.confStr = confStr;
        if (StringUtils.isNotEmpty(principal) && StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(confStr)) {
            this.iskerberos = true;
        }
        getConnection();
        return null;
    }

    public HBaseAdmin getHbaseAdmin() throws IOException {
        return (HBaseAdmin) getConnection().getAdmin();
    }

    public Connection getHBaseConn() {
        return getConnection();
    }

    public void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getZkHostAndPort() {
        return zkHostAndPort;
    }

    public void setZkHostAndPort(String zkHostAndPort) {
        this.zkHostAndPort = zkHostAndPort;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

}
