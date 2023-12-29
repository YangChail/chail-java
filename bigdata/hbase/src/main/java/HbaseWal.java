import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;

import java.io.File;
import java.io.IOException;

public class HbaseWal {


    public static void main(String[] args) throws IOException {
        String getenv = System.getProperty("user.dir");
        String pathStr=getenv+File.separator+"bigdata"+File.separator+"hbase"+File.separator+"src"+File.separator+"resources"+File.separator+"pv2-00000000000000000001.log";

        Configuration config = HBaseConfiguration.create();

        Path walFile = new Path(pathStr);


        WAL.Reader reader = WALFactory.createReader(walFile.getFileSystem(config), walFile, config);

        try {
            WAL.Entry entry = null;
            while ((entry = reader.next()) != null) {
                // 处理每个entry
                WALEdit edit = entry.getEdit();
                for (Cell cell : edit.getCells()) {
                    // 处理每个cell
                    byte[] row = CellUtil.cloneRow(cell);
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    long timestamp = cell.getTimestamp();
                    KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp, value);
                    // 处理keyValue
                }
            }
        } finally {
            reader.close();
        }


       // WALPrettyPrinter.main(new String[]{path});
    }
}
