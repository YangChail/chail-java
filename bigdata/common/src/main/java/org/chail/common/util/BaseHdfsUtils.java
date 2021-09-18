package org.chail.common.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.chail.common.krb.KerberosLoginSubject;
import org.chail.common.krb.KrbConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName : BaseKerberos
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-02 20:05
 */
public class BaseHdfsUtils  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseHdfsUtils.class);

    public static ReentrantLock lock = new ReentrantLock();


    private long lastPrintLogTime=0L;
    private long lineNum=0;
    /**
     * 间隔打印时间30秒
     */
    private static final long printLogTime=30*1000;



    /**
     * 文件路径
     */
    protected String path;

    /**
     * 是否是kerberos
     */
    protected boolean isKerberos;

    /**
     * 配置文件
     */
    protected Configuration configuration;



    public BaseHdfsUtils(String path, Configuration configuration) throws Exception {
        Configuration configurationres =HadoopConstant.setConfig(path,configuration);
        isKerberos= KrbConstant.isKerberosEnbale(configurationres);
        this.configuration = configurationres;
        this.path = path;
    }

    public BaseHdfsUtils() throws Exception {

    }

    /**
     * 初始化
     *
     * @param path
     * @throws Exception
     */
    public BaseHdfsUtils(String path) throws Exception {
        if(StringUtils.isEmpty(path)){
            throw new Exception("路径为空！");
        }
        this.path = path;
        if (!path.startsWith("hdfs://")) {
            configuration = new Configuration();
        } else {
            configuration = HadoopConstant.setConfig(path, configuration);
            isKerberos= KrbConstant.isKerberosEnbale(configuration);
        }
    }


    public Configuration getConfiguration() {
        return configuration;
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
        if (!path.startsWith("hdfs://")) {
            return UserGroupInformation.createRemoteUser("hive");
        }
        if (isKerkeros) {
            return KerberosLoginSubject.loginFromSubject(path, KrbConstant.getKerberosPrincipal(configuration),
                KrbConstant.getKerberosKeytab(configuration), KrbConstant.getKerberoskrb5(configuration));
        } else {
            return KerberosLoginSubject.loginFromSubject(path, "", "", "");
        }
    }


    public FileSystem getFileSystem(String path, Configuration configuration) throws IOException, URISyntaxException {
        UserGroupInformation.setConfiguration(configuration);
        if (!path.startsWith("hdfs://") && !path.startsWith("/")) {
            //本地文件系统
            path = "/" + path;
        }
        return FileSystem.get(new URI(path), configuration);
    }


    /**
     * 关闭文件系统
     *
     * @param fileSystem
     */
    public void closeFileSystem(FileSystem fileSystem) {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (Exception e) {
            LOGGER.warn("关闭文件系统报错 -{}", fileSystem.getUri().getPath());
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
                    return getFileSystem(path.toString(), configuration);
                }
            });
        });
    }


    /**
     * 删除缓存文件
     *
     * @param filePath
     */
    public static void deleteTmpFile(String filePath) {
        if (!filePath.startsWith("hdfs://")) {
            LOGGER.info("开始删除 mapreduce 运行后的缓存文件,如crc文件等");
            try {
                File f = new File(filePath);
                String name = f.getName();
                File tmp = new File(f.getParent() + File.separator + "." + name + ".crc");
                if (tmp.exists()) {
                    tmp.delete();
                }
            } catch (Exception e) {
                LOGGER.warn("删除 mapreduce 运行后的缓存文件失败");
            }
        }
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
                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    try {
                        boolean exist = fileSystem.exists(path);
                        if (exist && override) {
                            return fileSystem.delete(path, true);
                        }
                        return !exist;
                    } finally {
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
     * @return
     * @throws Exception
     */
    public synchronized Path makeQualified(Path path) throws Exception {
        return lock(() -> {
            return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Path>() {
                @Override
                public Path run() throws Exception {

                    FileSystem fileSystem = FileSystem.newInstance(new URI(path.toString()), configuration);
                    try {
                        Path makeQualified = fileSystem.makeQualified(path);
                        return makeQualified;
                    } finally {
                        closeFileSystem(fileSystem);
                    }
                }
            });
        });
    }


    /**
     * 创建文件夹 只有创建成功的情况下返回true
     *
     * @param path
     * @return
     * @throws Exception
     */
    public synchronized boolean mkdirs(Path path) throws Exception {
        return lock(() -> {
            return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws Exception {

                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    try {
                        boolean exist = fileSystem.exists(path);
                        if (!exist) {
                            return fileSystem.mkdirs(path);
                        }
                        return false;
                    } finally {
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
    public synchronized FileStatus[] listStatus(Path path) throws Exception {
        return lock(() -> {
            return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<FileStatus[]>() {
                @Override
                public FileStatus[] run() throws Exception {
                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    try {
                        return fileSystem.listStatus(path);
                    } catch (Exception e1) {
                        String message = e1.getMessage();
                        String[] split = message.split("\n\t");
                        if (split.length > 1) {
                            message = split[0];
                        }
                        throw new Exception("获取文件错误,原因:" + message);
                    } finally {
                        closeFileSystem(fileSystem);
                    }
                }
            });
        });
    }


    /**
     * 关闭所有流
     * @param closeables
     * @throws Exception
     */
    public void closeCloseableStream(Closeable... closeables)
        throws Exception {
        lock(() -> {
            getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    for (Closeable closeable : closeables) {
                        try {
                            if (closeable != null) {
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

                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    try {
                        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
                        while (iterator != null && iterator.hasNext()) {
                            LocatedFileStatus next = iterator.next();
                            if (filterFile(next.getPath().getName())) {
                                files.add(next.getPath().toString());
                            }
                        }
                        return files;
                    } catch (FileNotFoundException e) {
                        LOGGER.warn("文件不存在,"+e.getMessage());
                        return files;
                    } finally {
                        closeFileSystem(fileSystem);
                    }
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
                    AtomicBoolean exists =new AtomicBoolean(false);
                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    AtomicReference<Exception> resee=new AtomicReference();;
                    try {
                        Thread t1=new Thread(()->{
                            try {
                                exists.set(fileSystem.exists(path));
                            } catch (Exception e) {
                                if (e != null && e.getMessage().indexOf("not supported in state standby") > -1) {
                                    LOGGER.debug("HOST:{} IS STANDBY", uri.getHost());
                                    exists.set(false);
                                }else{
                                    resee.set(e);
                                }
                            }
                        });
                        t1.start();
                        try {
                            t1.join(30000);    //在主线程中等待t1执行2秒
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }finally {
                            t1.interrupt();
                        }
                        if(resee.get()!=null){
                            throw resee.get();
                        }
                    } finally {
                        closeFileSystem(fileSystem);
                    }
                    return exists.get();
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


    public boolean isKerberos() {
        return isKerberos;
    }

    public void setKerberos(boolean isKerberos) {
        this.isKerberos = isKerberos;
    }


}
