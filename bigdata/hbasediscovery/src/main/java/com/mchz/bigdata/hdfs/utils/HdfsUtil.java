package com.mchz.bigdata.hdfs.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class HdfsUtil {
	private Configuration configuration;
	private String path;
	private boolean isKerberos;
	public static final String CORE_SITE_FILE_NAME = "core-site.xml";
	public static final String HDFS_SITE_NAME = "hdfs-site.xml";
	public static final String YARN_SITE_NAME = "yarn-site.xml";
	public static final String MAPRED_SITE_NAME = "mapred-site.xml";
	public static final String KRB5_FILE_NAME = "krb5.conf";
	public static final String HBASE_SITE_FILE_NAME = "hbase-site.xml";
	public static ConcurrentHashMap<String, Map<String, String>> CONFIG_AND_SUBJECT_MAP = new ConcurrentHashMap<>();
	public static final String[] CONFIG_ARRAY = { CORE_SITE_FILE_NAME, HDFS_SITE_NAME, YARN_SITE_NAME,
			MAPRED_SITE_NAME };
	public static ReentrantLock lock = new ReentrantLock();
	private static final Logger LOGGER = LoggerFactory.getLogger(KerboersUtils.class);

	public HdfsUtil(String path ,Configuration configuration) throws Exception{
		this.configuration=configuration;
		this.path = path;
		init();
	}

	public HdfsUtil() throws Exception{

	}


	private void init() throws Exception{
		if(configuration==null) {
		configuration = new Configuration();
		}
		URI url = new URI(path);
		String authority = url.getAuthority();
		if(authority==null) {
			return;
		}
		Map<String, String> map = CONFIG_AND_SUBJECT_MAP.get(authority);
		if (map == null || map.size() < 1) {
			isKerberos=false;
			LOGGER.error("配置文件为空,文件路径为 "+path);
			return;
			//throw new IOException("配置文件为空,文件路径为 "+path);
		}
		addConfig(map, configuration);
		configuration.set("fs.hdfs.impl.disable.cache", "true");
		isKerberos = KerboersUtils.isKerberosEnbale(configuration);
		if (isKerberos) {
			KerboersUtils.setKerberosSubjectInfo(configuration, map);
		}
	}

	/**
	 * 初始化
	 *
	 * @param path
	 * @throws Exception
	 */
	public HdfsUtil(String path) throws Exception {
		this.path = path;
		if(!path.startsWith("hdfs://")) {
			configuration = new Configuration();
		}else {
		init();
	}
	}


	/**
	 * 关闭文件系统
	 * @param fileSystem
	 */
	public void closeFileSystem(FileSystem fileSystem) {
		try {
			if(fileSystem!=null) {
				fileSystem.close();
			}
		} catch (Exception e) {
			LOGGER.warn("关闭文件系统报错 -{}",fileSystem.getUri().getPath());
		}

	}

	/**
	 * 获取ugi
	 *
	 * @param isKerkeros
	 * @return
	 * @throws Exception
	 */
	public synchronized UserGroupInformation getUgi(boolean isKerkeros) throws Exception {
		//本地文件
		if(!path.startsWith("hdfs://")) {
			return UserGroupInformation.createRemoteUser("hive");
		}
		if (isKerkeros) {
			return KerboersUtils.getLoginUgi(path, KerboersUtils.getKerberosPrincipal(configuration),
					KerboersUtils.getKerberosKeytab(configuration), KerboersUtils.getKerberoskrb5(configuration),
					configuration);
		} else {
			return KerboersUtils.getLoginUgi(path, "", "", "", configuration);
		}
	}

	/**
	 * 获取文件系统
	 *
	 * @return
	 * @throws Exception
	 */
	public synchronized FileSystem getFileSystem() throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<FileSystem>() {
				@Override
				public FileSystem run() throws IOException, URISyntaxException {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					return getFileSystem(path.toString(),configuration);
				}
			});
		});
	}

	/**
	 * 判断文件是否存在
	 *
	 * @param path
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public synchronized boolean exists(Path path) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Boolean>() {
				@Override
				public Boolean run() throws Exception {
					String string = path.toString();
					URI uri = new URI(string);
					configuration.set("fs.hdfs.impl.disable.cache", "true");
                    FileSystem fileSystem =getFileSystem(path.toString(),configuration);
					try {
						boolean exists = fileSystem.exists(path);
						return exists;
					} catch (RemoteException e) {
						if(e.getMessage().indexOf("not supported in state standby")>-1) {
							LOGGER.warn("HOST:{} IS STANDBY",uri.getHost());
							return false;
						}
						throw e;
					}finally {
						closeFileSystem(fileSystem);
					}
				}
			});
		});
	}





	public synchronized Path getHomeDirectory() throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Path>() {
				@Override
				public Path run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
                    FileSystem fileSystem =getFileSystem(path.toString(),configuration);
					try {
						return fileSystem.getHomeDirectory();
					}finally {
						closeFileSystem(fileSystem);
					}
				}
			});
		});
	}

	/**
	 * 查询所有文件
	 *
	 * @param path
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public synchronized FileStatus[] listStatus(Path path) throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<FileStatus[]>() {
				@Override
				public FileStatus[] run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fileSystem =getFileSystem(path.toString(),configuration);
					try {
					return fileSystem.listStatus(path);
					} catch (Exception e1) {
						String message = e1.getMessage();
						String[] split = message.split("\n\t");
						if(split.length>1) {
							message=split[0];
				}
						throw new Exception("获取文件错误,原因:"+message) ;
					}finally {
                        closeFileSystem(fileSystem);
                    }
				}
			});
		});
	}

	/**
	 * 获取所有单个文件
	 *
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public synchronized FileStatus getFileStatus(Path path) throws Exception {
		FileStatus[] listStatus = listStatus(path);
		FileStatus fileStatus = null;
		if (listStatus.length > 0) {
			fileStatus = listStatus[0];
		}
		return fileStatus;
	}

	/**
	 * 获取路径下所有非目录文件
	 *
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public synchronized List<String> getAllFiles(Path path) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<List<String>>() {
				@Override
				public List<String> run() throws Exception {
					List<String> files = new ArrayList<>();
					configuration.set("fs.hdfs.impl.disable.cache", "true");
                    FileSystem fileSystem =getFileSystem(path.toString(),configuration);
					try {
						RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
						while (iterator != null && iterator.hasNext()) {
							LocatedFileStatus next = iterator.next();
							if (filterFile(next.getPath().getName())) {
								files.add(next.getPath().toString());
							}
						}
						return files;
					}finally {
						closeFileSystem(fileSystem);
					}
				}
			});
		});
	}

	/**
	 * 过滤系统文件
	 *
	 * @param name
	 */
	public boolean filterFile(String name) {
		if (name.startsWith(".")) {
			return false;
		}
		if (name.contains("_impala_insert_staging")) {
			return false;
		}
		if (name.contains("_SUCCESS")) {
			return false;
		}
		return true;
	}

	/**
	 * 打开文件系统流
	 *
	 * @param path
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public synchronized InputStream open(Path path) throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<InputStream>() {
				@Override
				public InputStream run() throws Exception {
					FileSystem fs =getFileSystem(path.toString(),configuration);
					return fs.open(path);
				}
			});
		});

	}

	/**
	 * 打开文件系统流
	 *
	 * @param path
	 * @param bufferSize
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public synchronized InputStream open(Path path, int bufferSize) throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<InputStream>() {
				@Override
				public InputStream run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fs =getFileSystem(path.toString(),configuration);
					return fs.open(path, bufferSize);
				}
			});
		});
	}

	/**
	 * 获取输出流
	 *
	 * @param path
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public synchronized OutputStream create(Path path) throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<OutputStream>() {
				@Override
				public OutputStream run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fs =getFileSystem(path.toString(),configuration);
					return fs.create(path);
				}
			});
		});
	}

	/**
	 * 创建输出流
	 *
	 * @param path
	 * @param overWrite
	 * @return
	 * @throws Exception
	 */
	public synchronized OutputStream create(Path path, boolean overWrite) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<OutputStream>() {
				@Override
				public OutputStream run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fs =getFileSystem(path.toString(),configuration);
					return fs.create(path, overWrite);
				}
			});
		});
	}

	/**
	 * 创建输出流
	 *
	 * @param path
	 * @param overWrite
	 * @param bufferSize
	 * @param replication
	 * @param blockSize
	 * @param progress
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public OutputStream create(Path path, boolean overWrite, int bufferSize, short replication, long blockSize,
			Progressable progress) throws  Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<OutputStream>() {
				@Override
				public OutputStream run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fileSystem = FileSystem.get(new URI(path.toString()), configuration);
					return fileSystem.create(path, overWrite, bufferSize, replication, blockSize, progress);
				}
			});
		});
	}


	public void closeCloseableStream(Closeable... closeables )
			throws Exception {
		lock(() -> {
			getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Object>() {
				@Override
				public Object run() throws Exception {
					for(Closeable closeable:closeables) {
						try {
						if(closeable!=null) {
			 				closeable.close();
						}
						} catch (Exception e) {
					}

					}
					return null;
				}
			});
		});
	}

	/**
	 * 删除文件 如果文件存在
	 *
	 * @param path
	 * @param override
	 * @return
	 * @throws Exception
	 */
	public synchronized boolean deleteIfExist(Path path, boolean override) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Boolean>() {
				@Override
				public Boolean run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
                    FileSystem fileSystem =getFileSystem(path.toString(),configuration);
					try {
						boolean exist = fileSystem.exists(path);
						if (exist && override) {
							return fileSystem.delete(path, true);
						}
						return !exist;
					}finally {
						closeFileSystem(fileSystem);
					}
				}
			});
		});
	}



	/**
	 * 删除文件 如果文件存在
	 *
	 * @param path
	 * @param override
	 * @return
	 * @throws Exception
	 */
	public synchronized Path makeQualified(Path path) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Path>() {
				@Override
				public Path run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					FileSystem fileSystem = FileSystem.newInstance(new URI(path.toString()), configuration);
					try {
						Path makeQualified = fileSystem.makeQualified(path);
						return makeQualified;
					}finally {
						closeFileSystem(fileSystem);
					}
				}
			});
		});
	}


	public Job getJobInstance(Configuration configuration) throws Exception {
		return lock(() -> {
			return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Job>() {
				@Override
				public Job run() throws Exception {
					configuration.set("fs.hdfs.impl.disable.cache", "true");
					return Job.getInstance(configuration);
				}
			});
		});
	}


	public FileSystem getFileSystem(String path,Configuration configuration) throws IOException, URISyntaxException {
		if(!path.startsWith("hdfs://")&&!path.startsWith("/")) {
			//本地文件系统
			path="/"+path;
		}
		return FileSystem.get(new URI(path), configuration);
	}


	public long getDefaultBlockSize(Path path) throws Exception {
		return getFileSystem().getDefaultBlockSize(path);
	}

	public short getDefaultReplication(Path path) throws Exception {
		return getFileSystem().getDefaultReplication(path);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public static void addConfig(Map<String, String> kerberosSubjectMap, Configuration conf) {
		for (String str : CONFIG_ARRAY) {
			String string = kerberosSubjectMap.get(str);
			if (StringUtils.isNotEmpty(string)&&conf.toString().indexOf(string) < 0) {
				Path uri = new Path(string);
				//LOGGER.info("config ..{}",string);
				conf.addResource(uri,false);
			}
		}
	}

	public boolean isKerberos() {
		return isKerberos;
	}

	public void setKerberos(boolean isKerberos) {
		this.isKerberos = isKerberos;
	}

	/**
	 * 删除缓存文件
	 * @param filePath
	 */
	public static void deleteTmpFile(String filePath) {
		if(!filePath.startsWith("hdfs://")) {
			LOGGER.info("开始删除 mapreduce 运行后的缓存文件,如crc文件等");
			try {
				File f=new File(filePath);
				String name = f.getName();
				File tmp=new File(f.getParent()+File.separator+"."+name+".crc");
				if(tmp.exists()) {
					tmp.delete();
				}
			} catch (Exception e) {
				LOGGER.warn("删除 mapreduce 运行后的缓存文件失败");
			}
		}
	}

	protected <R> R lock(SupplierWithException<R> action) throws Exception {
		lock.lock();
		try {
			return action.get();
		} finally {
			lock.unlock();
		}

	}

	protected <R> void lock(RunnableWithException<R> action) throws Exception {
		lock.lock();
		try {
			action.get();
		} finally {
			lock.unlock();
		}
	}

	@FunctionalInterface
	public static abstract interface RunnableWithException<T> {
		public abstract void get() throws Exception;
	}

	@FunctionalInterface
	public static abstract interface SupplierWithException<T> {
		public abstract T get() throws Exception;
	}

}
