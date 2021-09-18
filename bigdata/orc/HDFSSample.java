package com.chail.apputil.orc;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.io.orc.Metadata;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
/**
 * Created by Administrator on 2017/11/7.s
 * API2
 * 参数为需要兑换表的字段，去跟码表的第一个字段去匹配，兑换出码表的第二个字段
 */
public class HDFSSample extends UDF {
    public static String evaluate(String pro_id) throws IOException {
        String INPUT = "/1";
        Configuration conf = new Configuration();
        Path file_in = new Path(INPUT);
        Reader reader = OrcFile.createReader(FileSystem.get(URI.create(INPUT), conf), file_in);
        List<String> metadata = reader.getMetadataKeys();
        ByteBuffer metadataValue = reader.getMetadataValue( "column 0" );
        String byteBufferToString = byteBufferToString( metadataValue, Charset.forName( "UTF-8" ) );
        System.out.println(byteBufferToString);
        
        StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
        RecordReader records = reader.rows();
        Object row = null;
        Map<String,String> datamap = new HashMap<String, String>();
        while (records.hasNext()) {
            row = records.next(row);
            List value_lst = inspector.getStructFieldsDataAsList(row);
            String string = value_lst.get(0).toString();
           //String string2 = value_lst.get(1).toString();
            System.out.println(string);
           // System.out.println(string2);
        }
        return  "";
    }
    
    public static void main(String[] args) throws IOException {
    	HDFSSample.evaluate("111111");
	}
    
    
    private static String byteBufferToString( ByteBuffer buffer, Charset charset ) {
        byte[] bytes;
        if ( buffer.hasArray() ) {
          bytes = buffer.array();
        } else {
          bytes = new byte[ buffer.remaining() ];
          buffer.get( bytes );
        }
        return new String( bytes, charset );
      }
 
}