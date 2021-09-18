package com.chail.apputil.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetConverter;



public class ParquteTest {
	public static final String PRINCIPAL = "principal";
	public static final String KEYTAB = "keytab";
	public static final String CONF = "conf";
	

	public static final String CORE_SITE_FILE_NAME = "core-site.xml";
	public static final String HDFS_SITE_NAME = "hdfs-site.xml";
	public static final String YARN_SITE_NAME = "yarn-site.xml";
	public static final String MAPRED_SITE_NAME = "mapred-site.xml";

	public static final String[] CONFIG_ARRAY = { CORE_SITE_FILE_NAME, HDFS_SITE_NAME, YARN_SITE_NAME,
			MAPRED_SITE_NAME };
	
	protected static ConcurrentHashMap<String, List<String>> CONFIG_MAP = new ConcurrentHashMap<>();

	public static void main(String[] args) throws Exception {
		String path1="hdfs://192.168.200.167:8020/user/hive/warehouse/yccout.db/data1w_par/000000_0";
		System.setProperty("sun.security.krb5.debug", "true");
		List<String> configPath=CONFIG_MAP.get(new URI(path1).getAuthority());
		Configuration conf = new Configuration();
		for(String confpath:configPath) {
			Path uri = new Path(confpath);
			conf.addResource(uri);
		}
		//conf.set("hadoop.security.authentication", "kerberos");
		
		Path file = new Path(path1);
	
		loginCheckAndAddConfigReturnUGI(new URI(path1),conf);
	
		Job job = Job.getInstance(conf);
		ParquetInputFormat.setInputPaths(job, file.getParent());
		ParquetInputFormat.setInputDirRecursive(job, false);
		//ParquetInputFormat.setInputPathFilter(job, ReadFileFilter.class);
		UserGroupInformation ugi = loginCheckAndAddConfigReturnUGI(new URI(path1),job.getConfiguration());
		ugi.doAs(new PrivilegedAction<FileSystem>() {
			@Override
			public FileSystem run() {
				List<InputSplit> splits = getSplits(path1,job);
				try {
					MessageType readSchema = readSchema(path1,job.getConfiguration());
					ParquetRecordReader<ParquetData> createRecordReader = createRecordReader(readSchema,splits.get(0),job.getConfiguration());
					
					while (createRecordReader.nextKeyValue()) {
						ParquetData currentValue = createRecordReader.getCurrentValue();
						System.out.println();
					}
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				System.out.println();
				
				
				return 	null;
			}
		});
		
		
		
		
		
		
		
	
	}

	public static ParquetRecordReader<ParquetData> createRecordReader(MessageType readSchema ,InputSplit split,Configuration conf) throws Exception {
		
		MyParquetReadSupport readSupport = new MyParquetReadSupport(readSchema);
		
		
		
		
		ParquetRecordReader<ParquetData> nativeRecordReader = new ParquetRecordReader<ParquetData>(readSupport,
				ParquetInputFormat.getFilter(conf));
		
			
		TaskAttemptContextImpl task = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		nativeRecordReader.initialize(split, task);
		return nativeRecordReader;
	}

	public static List<InputSplit>  getSplits(String path,	Job job ) {
		try {
			ParquetInputFormat.setInputPaths(job, path);
			ParquetInputFormat.setInputDirRecursive(job, true);
			ParquetInputFormat<ParquetData> nativeParquetInputFormat = new ParquetInputFormat<>();
			return nativeParquetInputFormat.getSplits(job);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 通过文件地址从缓存拿出登陆
	 * 
	 * @param path
	 * @param conf
	 * @throws IOException
	 */
	public static UserGroupInformation loginCheckAndAddConfigReturnUGI(URI path, Configuration conf) throws IOException {
		String string = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION);
		if (string != null && "KERBEROS".equalsIgnoreCase(string)) {
			UserGroupInformation.setConfiguration(conf);
			String key = path.getAuthority();
			Map<String, String> config = configMap.get(key);
			if (config != null) {
				try {
					System.setProperty("java.security.krb5.conf", config.get(CONF));
					UserGroupInformation loginUserFromKeytabAndReturnUGI = UserGroupInformation
							.loginUserFromKeytabAndReturnUGI(config.get(PRINCIPAL), config.get(KEYTAB));
					// logger.info(key + " kerberos login success");
					return loginUserFromKeytabAndReturnUGI;
				} catch (Exception e) {
					// logger.error("kerberos login error", e);
					return null;
				}
			}
		}
		return UserGroupInformation.getCurrentUser();
	}
	
	
	
	public static Map<String,HashMap<String, String>> configMap=new HashMap<String,HashMap<String, String>>();
	static {
		String user1 = "hdfs/master@HADOOP.COM";
		String keytab1 = "D:\\hadoop-conf\\18\\hdfs.keytab";
		String dir1 = "hdfs://192.168.200.18:8020/";
		String confStr1 = "D:\\hadoop-conf\\18\\krb5.conf";
		String confpath1 ="D:\\hadoop-conf\\18";
		addConfigPathToCash(dir1,confpath1);
		HashMap<String, String> config = new java.util.HashMap<>();
		config.put(PRINCIPAL,user1);
		config.put(KEYTAB,keytab1);
		config.put(CONF,confStr1);
		try {
			configMap.put(new URI(dir1).getAuthority(), config);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String user = "hdfs/slave1@CDH167.COM";
		String keytab = "D:\\hadoop-conf\\167\\hdfs.keytab";
		String dir = "hdfs://192.168.200.167:8020/";
		String confStr ="D:\\hadoop-conf\\167\\krb5.conf";
		String confpath ="D:\\hadoop-conf\\167";
		
		HashMap<String, String> config1 = new java.util.HashMap<>();
		config1.put(PRINCIPAL,user);
		config1.put(KEYTAB,keytab);
		config1.put(CONF,confStr);
		addConfigPathToCash(dir,confpath);
		
		
		try {
			configMap.put(new URI(dir).getAuthority(), config1);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
	public static MessageType readSchema(String file,Configuration conf) throws Exception {
			Path path=new Path(file);
			FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
			FileStatus fileStatus = fileSystem.getFileStatus(path);
			List<Footer> footers = ParquetFileReader.readFooters(conf, fileStatus, true);
		
			ParquetMetadata meta = footers.get(0).getParquetMetadata();
			 
			MessageType schema = meta.getFileMetaData().getSchema();
			
			return schema;
	}
	
	
	
	
	public static void addConfigPathToCash(String connectionString, String configFilePath) {
		String authority;
		try {
			authority = new URI(connectionString).getAuthority();
			List<String> configPath = new ArrayList<String>();
			for (String str : CONFIG_ARRAY) {
				String path = configFilePath + File.separator + str;
				File f = new File(path);
				if (f.exists()) {
					configPath.add(path);
				}
			}
			CONFIG_MAP.put(authority, configPath);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

}
