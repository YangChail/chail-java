package com.mchz.gzip;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.util.StringUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * @Title: GZIPUtil.java
 * @Description: gzip文件压缩和解压缩工具类
 * @author LM
 * @date 2009-11-4 下午06:23:29
 * @version V1.0
 */
public class GZIPUtil {


    private static String localpath = System.getProperty("user.dir");


    /**
     *
     * @Title: pack
     * @Description: 将一组文件打成tar包
     * @param sources 要打包的原文件数组
     * @param target 打包后的文件
     * @return File    返回打包后的文件
     * @throws
     */
    public static File pack(File[] sources, File target){
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(target);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        TarArchiveOutputStream os = new TarArchiveOutputStream(out);
        for (File file : sources) {
            try {
                os.putArchiveEntry(new TarArchiveEntry(file));
                IOUtils.copy(new FileInputStream(file), os);
                os.closeArchiveEntry();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(os != null) {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return target;
    }


    public static boolean gzipCompression(String filePath, String resultFilePath) throws IOException {
        System.out.println(" gzipCompression -> Compression start!");
        InputStream fin = null;
        BufferedInputStream bis = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos= null;
        GzipCompressorOutputStream gcos = null;
        try {
            fin = Files.newInputStream(Paths.get(filePath));
            bis = new BufferedInputStream(fin);
            fos = new FileOutputStream(resultFilePath);
            bos = new BufferedOutputStream(fos);
            gcos = new GzipCompressorOutputStream(bos);
            byte[] buffer = new byte[1024];
            int read = -1;
            while ((read = bis.read(buffer)) != -1) {
                gcos.write(buffer, 0, read);
            }
        } finally {
            if(gcos != null)
                gcos.close();
            if(bos != null)
                bos.close();
            if(fos != null)
                fos.close();
            if(bis != null)
                bis.close();
            if(fin != null)
                fin.close();
        }
        System.out.println(" gzipCompression -> Compression end!");
        return true;
    }


    /**
     *
     * @Title: compress
     * @Description: 将文件用gzip压缩
     * @param  source 需要压缩的文件
     * @return File    返回压缩后的文件
     * @throws
     */
    public static File compress(File source) {
        File target = new File(source.getParent()+File.separator+source.getName().substring(0,source.getName().lastIndexOf(".")) + ".tgz");
        FileInputStream in = null;
        GZIPOutputStream out = null;
        try {
            in = new FileInputStream(source);
            out = new GZIPOutputStream(new FileOutputStream(target));
            byte[] array = new byte[1024];
            int number = -1;
            while((number = in.read(array, 0, array.length)) != -1) {
                out.write(array, 0, number);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            if(out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
        return target;
    }


    public static void unzip(String tagepath,String filename)  {
        //读取压缩文件
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(filename), Charset.forName("GBK"))) {
            //zip文件实体类
            ZipEntry entry;
            //遍历压缩文件内部 文件数量
            while ((entry = in.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    //文件输出流
                    FileOutputStream out = new FileOutputStream(tagepath+File.separator + entry.getName());
                    BufferedOutputStream bos = new BufferedOutputStream(out);
                    int len;
                    byte[] buf = new byte[1024];
                    while ((len = in.read(buf)) != -1) {
                        bos.write(buf, 0, len);
                    }
                    // 关流顺序，先打开的后关闭
                    bos.close();
                    out.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 压缩 .tar.gz 文件
     *
     * @param outPath      目标文件
     * @throws Exception 异常
     */
    public static void packet(File resourceFile, String outPath) throws Exception {


        // 迭代源文件集合，将文件打包为 tar
        try (FileOutputStream fileOutputStream = new FileOutputStream(outPath + ".tmp");
             BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);
             TarOutputStream tarOutputStream = new TarOutputStream(bufferedOutput)) {
                FileInputStream fileInputStream = new FileInputStream(resourceFile);
                BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
                TarEntry entry = new TarEntry(new File(resourceFile.getName()));
                entry.setSize(resourceFile.length());
                tarOutputStream.putNextEntry(entry);
                IOUtils.copy(bufferedInput, tarOutputStream);
                tarOutputStream.closeEntry();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 读取打包好的 tar 临时文件，使用 GZIP 方式压缩
        try (FileInputStream fileInputStream = new FileInputStream(outPath + ".tmp");
             BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
             FileOutputStream fileOutputStream = new FileOutputStream(outPath+".tgz");
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
             BufferedOutputStream bufferedOutput = new BufferedOutputStream(gzipOutputStream);
        ) {
            byte[] cache = new byte[1024];
            for (int index = bufferedInput.read(cache); index != -1; index = bufferedInput.read(cache)) {
                bufferedOutput.write(cache, 0, index);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Files.delete(Paths.get(outPath + ".tmp"));
        }
    }



    public static void makeImage(String soucePath,String tagePath) throws IOException {
        makeImage(soucePath,tagePath,true);
    }

    public static void makeImage(String soucePath,String tagePath,boolean iscopy) throws IOException {
        File file= new File(tagePath);
        if(iscopy){
            FileUtils.copyFile(new File(soucePath),file);
        }
        String tar = tagePath.substring(0, tagePath.lastIndexOf("."));
        try {
            packet(file,tar);
        } catch (Exception e) {
            e.printStackTrace();
        }
       // compress(file);
        System.out.println("制作成功   --"+tagePath);
        file.delete();
    }


    public static void main(String[] args) throws IOException {
        String tagepath=localpath+File.separator+new SimpleDateFormat("yyyy_MMdd_HH_mm_ss").format(new Date())+"_out";
        System.out.println("目标文件夹  "+tagepath);
        String dscommonPath = tagepath + File.separator + "dscommon.tar";
        String manager01Path = tagepath + File.separator + "dsmanager01.tar";
        String manager02Path = tagepath + File.separator + "dsmanager02.tar";
        String engine01Path = tagepath + File.separator + "dsengine01.tar";
        String engine02Path = tagepath + File.separator + "dsengine02.tar";



        String dsinc = tagepath + File.separator + "dsinc.tar";
        String dsinc01 = tagepath + File.separator + "dsinc01.tar";
        String dsinc02 = tagepath + File.separator + "dsinc02.tar";
        String dsbezium01 = tagepath + File.separator + "debezium01.tar";
        String dsbezium02 = tagepath + File.separator + "debezium02.tar";
        String dsync01 = tagepath + File.separator + "dsync01.tar";
        String dsync02 = tagepath + File.separator + "dsync02.tar";

        String dswebs = tagepath + File.separator + "dsweb.tar";
        String dscenter = tagepath + File.separator + "dscenter.tar";


        File file= new File(localpath);
        File[] files = file.listFiles();
        File target=new File(tagepath);
        target.mkdir();
        for(File zipfile:files){
            if(zipfile.getName().equals("dscommon.zip")){
                System.out.println("dscommon.zip正在解压...");
                unzip(tagepath,zipfile.getPath());
                makeImage(dscommonPath,manager01Path);
                makeImage(dscommonPath,manager02Path);
                makeImage(dscommonPath,engine01Path);
                makeImage(dscommonPath,engine02Path);
            }else if(zipfile.getName().equals("dsinc.zip")){
                System.out.println("dsinc.zip正在解压...");
                unzip(tagepath,zipfile.getPath());
                makeImage(dsinc,dsinc01);
                makeImage(dsinc,dsinc02);
                makeImage(dsinc,dsbezium01);
                makeImage(dsinc,dsbezium02);
                makeImage(dsinc,dsync01);
                makeImage(dsinc,dsync02);
            }else if(zipfile.getName().equals("dsweb.zip")){
                System.out.println("dsweb.zip正在解压...");
                unzip(tagepath,zipfile.getPath());
                makeImage(dswebs,dswebs,false);
            }else if(zipfile.getName().equals("dscenter.zip")){
                System.out.println("dscenter.zip正在解压...");
                unzip(tagepath,zipfile.getPath());
                makeImage(dscenter,dscenter,false);
            }
        }
        System.out.println("清除.tar");
        target.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if(pathname.getPath().endsWith("tar")){
                    pathname.delete();
                }
                return true;
            }
        });
        System.out.println("结束....");
    }
}

