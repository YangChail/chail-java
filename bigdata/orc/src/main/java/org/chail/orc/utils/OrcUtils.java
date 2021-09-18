package org.chail.orc.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.chail.orc.OrcField;
import org.chail.orc.input.MyOrcInputFormat;
import org.chail.orc.input.MyOrcRecordReader;
import org.chail.orc.output.TDHOrcFileds;
import org.pentaho.di.core.RowMetaAndData;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName : OrcUtils
 * @Description : orc获取有kerberos
 * @Author : Chail
 * @Date: 2020-11-02 20:26
 */
public class OrcUtils extends BaseHdfsUtils{


    public OrcUtils(String path, Configuration configuration) throws Exception {
        super(path, configuration);
    }

    public OrcUtils() throws Exception {
    }

    /**
     * 初始化
     *
     * @param path
     * @throws Exception
     */
    public OrcUtils(String path) throws Exception {
        super(path);
    }

    /**
     * 创建orc的写
     *
     * @param filePath
     * @param schema
     * @param conf
     * @return
     * @throws Exception
     */
    public Writer createOrcWirter(String filePath, TypeDescription schema, Configuration conf) throws Exception {
        return lock(() -> {
            return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Writer>() {
                @Override
                public Writer run() throws Exception {
                    OrcFile.WriterOptions setSchema = OrcFile.writerOptions(conf).setSchema(schema);
                    return OrcFile.createWriter(new Path(filePath), setSchema);
                }
            });
        });
    }


    /**
     * 获取orc的读取
     *
     * @param filePath
     * @param conf
     * @return
     * @throws Exception
     */
    public Reader getOrcReard(String filePath, Configuration conf) throws Exception {
        return lock(() -> {
            return getUgi(isKerberos).doAs(new PrivilegedExceptionAction<Reader>() {
                @Override
                public Reader run() throws Exception {
                    FileSystem fileSystem = getFileSystem(path.toString(), configuration);
                    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf).filesystem(fileSystem);
                    Reader createReader = OrcFile.createReader(new Path(filePath), readerOptions);
                     return createReader;
                }
            });
        });
    }


    /**
     * 获取输出字段
     * @param path
     */
    public  List<OrcField> getInputFeildsFromSchema(String path) throws Exception {
        List<OrcField> orcFields=new ArrayList<>();
        Reader orcReard = getOrcReard(path, getConfiguration());
        TypeDescription schema = orcReard.getSchema();
        List<TypeDescription> children = schema.getChildren();
        List<String> fieldNames = schema.getFieldNames();
        for (int i = 0; i < children.size(); i++) {
            TypeDescription child = children.get(i);
            if (child.getCategory() == TypeDescription.Category.STRUCT) {
                for (int k = 0; k < child.getChildren().size(); k++) {
                    orcFields.add(new OrcField(child.getFieldNames().get(k), child.getChildren().get(k).toString()));
                }
            } else {
                orcFields.add(new OrcField(fieldNames.get(i),  child.toString()));
            }
        }
        return orcFields;
    }





    public  Iterator<RowMetaAndData> getRowMetaAndData(String fileName) throws Exception {
        List<OrcField> inputFeildsFromSchema =getInputFeildsFromSchema(fileName);
        boolean torc = TDHOrcFileds.isTorc(inputFeildsFromSchema);
        List<OrcField> removetorcfiled = TDHOrcFileds.removetorcfiled(inputFeildsFromSchema);
        MyOrcInputFormat format = new MyOrcInputFormat(this, fileName);
        format.setTorc(torc);
        format.setConsumerInputFields(removetorcfiled);
        MyOrcRecordReader createRecordReader = format.createRecordReader();
        Iterator<RowMetaAndData> iterator = createRecordReader.iterator();
        return iterator;
    }



}
