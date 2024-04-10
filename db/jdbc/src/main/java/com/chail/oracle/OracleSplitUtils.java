package com.chail.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class OracleSplitUtils {
    protected static final Logger LOGGER = LoggerFactory.getLogger(OracleSplitUtils.class);


    /*MC_BATCH:10:select * from TEST2.A_DATA_1000W  where rowid BETWEEN '{x}' AND '{y}'
                    1.默认情况下 TEST2.A_DATA_1000W blockSize=8192 分了53片
                    2.配置了pageSize=100 blockSize=442368/100+1=4424 max(blockSize,8192)
                    3.分片sql样例 SELECT * FROM TEST2.A_DATA_1000W adw WHERE rowid BETWEEN 'AAS4QuAAEAAAAEAAAA' AND 'AAS4QuAAEAAACD/CcP'
                    */

    /**
     * 获取分片信息
     * MC_BATCH:10:select * from TEST2.A_DATA_1000W  where rowid BETWEEN '{x}' AND '{y}'
     *1.默认情况下 TEST2.A_DATA_1000W blockSize=8192 分了53片
     *2.配置了pageSize=100 blockSize=442368/100+1=4424 max(blockSize,8192)
     * 3.分片sql样例 SELECT * FROM TEST2.A_DATA_1000W adw WHERE rowid BETWEEN 'AAS4QuAAEAAAAEAAAA' AND 'AAS4QuAAEAAACD/CcP'
     *
     * @param pageSize
     * @param owner
     * @param table
     * @param con
     * @return 如果是一个空list,则表示没有分片
     * @throws Exception
     */
    public static List<String> buildSqlSplit( int pageSize,String owner, String table,  Connection con) throws Exception {
        List<String> splitChunkInfo;
        long blockSize=0;
        if (pageSize < 1) {
            splitChunkInfo = OracleSplitUtils.buildSplit(owner, table, blockSize, con);
        } else {
            String sql = String.format("select sum(BLOCKS) FROM dba_extents WHERE OWNER ='%s' AND segment_name = '%s'", owner, table);
            ResultSet resultSet = null;
            long totalBlockSize = 0;
            try {
                resultSet = con.createStatement().executeQuery(sql);
                while (resultSet.next()) {
                    totalBlockSize = resultSet.getLong(1);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
            }
            if(totalBlockSize == 0){
                return new ArrayList<>();
            }
            blockSize = totalBlockSize%pageSize == 0 ? totalBlockSize/pageSize : totalBlockSize/pageSize + 1;
            blockSize = Math.max(blockSize,8192);
            splitChunkInfo = OracleSplitUtils.buildSplitOptRow(owner, table, blockSize, con);
        }
        return splitChunkInfo;
    }


    public static List<String> buildSplitOptRow(String owner, String table, long blockSize, Connection con) throws Exception {
        long b = System.currentTimeMillis();
        PreparedStatement pso = null;
        PreparedStatement ps= null;
        PreparedStatement pss =null;
        List<String> result = new ArrayList();
        try{
//        Class.forName("oracle.jdbc.driver.OracleDriver");
//        Connection con=DriverManager.getConnection(
//            "jdbc:oracle:thin:@192.168.239.70:1521:orcl","ant","ant");
        //PreparedStatement ps = con.prepareStatement("select dbms_rowid.rowid_create(1,data_object_id,?,?,0) START_ROWID,dbms_rowid.rowid_create(1,data_object_id,?,?,9999) END_ROWID from dba_objects t WHERE OWNER = ? AND object_name = ?");
        //pso = con.prepareStatement("select data_object_id from dba_objects t WHERE OWNER = ? AND object_name = ? AND object_type='TABLE'");
        ps = con.prepareStatement("select dbms_rowid.rowid_create(1,?,?,?,0) START_ROWID,dbms_rowid.rowid_create(1,?,?,?,9999) END_ROWID from dual");
        //pss = con.prepareStatement("SELECT file_id,block_id,blocks,relative_fno FROM dba_extents WHERE OWNER =? AND segment_name=? ORDER BY file_id, BLOCK_ID");
        pss = con.prepareStatement("select file_id,block_id,blocks,relative_fno,o.DATA_OBJECT_ID FROM dba_extents e, dba_objects o WHERE o.owner = e.OWNER AND o.object_name = e.SEGMENT_NAME AND e.owner = ? AND e.segment_name = ? AND nvl(o.subobject_name,0) = nvl(e.partition_name,0) ORDER BY o.DATA_OBJECT_ID,e.file_id, e.BLOCK_ID");
        pss.setObject(1, owner);
        pss.setObject(2, table);
        ResultSet rs = pss.executeQuery();
        /*
        pso.setObject(1, owner);
        pso.setObject(2, table);
        ResultSet rso = pso.executeQuery();
        long objectId = 0;
        if(rso.next()) {
            objectId = rso.getLong(1);
        }
        */
        int bi = 0;
        int ci = 0;
        long blockCount = 0;
        long startIndex = 0 * blockSize;
        long endIndex = startIndex + blockSize;
        long b_objectId = 0;
        long b_fileId = 0;
        long b_blockId = 0;
        long b_blocks = 0;
        long b_relativeFno = 0;
        long objectId = 0;
        long fileId = 0;
        long blockId = 0;
        long blocks = 0;
        long relativeFno = 0;
        String startRowid = null;
        String endRowid = null;
        long startBlockIndex = 0;
        long blockIndex = 0;
        int flag = 0;
        while(rs.next()) {
            fileId = rs.getInt(1);
            blockId = rs.getInt(2);
            blocks = rs.getInt(3);
            relativeFno = rs.getInt(4);
            objectId = rs.getInt(5);
            startBlockIndex = blockCount;
            blockCount += blocks;
            LOGGER.debug("build:" + (ci++) + "," + blockCount + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno);
            while(blockIndex<blockCount) {
                if(flag == 0) {
                    if(blockCount>=startIndex) {
                        b_fileId = fileId;
                        b_blockId = blockId+(startIndex-startBlockIndex);
                        b_blocks = blocks;
                        b_relativeFno = relativeFno;
                        b_objectId = objectId;
                        LOGGER.debug("handle start:" + bi + "," + b_fileId + "," + b_blockId + "," + b_blocks + "," + b_relativeFno + "," + b_objectId);
                        flag = 1;
                    }else {
                        break;
                    }
                }
                if(flag == 1) {
                    if(blockCount>=endIndex) {
                        LOGGER.debug("handle end:" + bi + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno + "," + objectId);
                        ps.setObject(1, b_objectId);
                        ps.setObject(2, b_relativeFno);
                        ps.setObject(3, b_blockId);
                        ps.setObject(4, objectId);
                        ps.setObject(5, relativeFno);
                        ps.setObject(6, blockId+(endIndex-startBlockIndex)-1);
                        ResultSet rs1  = ps.executeQuery();
                        if(rs1.next()) {
                            startRowid = rs1.getString(1);
                            endRowid = rs1.getString(2);
                            result.add(startRowid + ":" + endRowid);
                        }
                        blockIndex=endIndex;
                        startIndex = endIndex;
                        endIndex = startIndex + blockSize;
                        flag = 0;
                        bi++;
                    }else {
                        break;
                    }
                }
            }
        }
        if(flag == 1) {
            LOGGER.debug("handle end:" + bi + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno + "," + objectId);
            ps.setObject(1, b_objectId);
            ps.setObject(2, b_relativeFno);
            ps.setObject(3, b_blockId);
            ps.setObject(4, objectId);
            ps.setObject(5, relativeFno);
            ps.setObject(6, blockId+blocks-1);
            ResultSet rs1  = ps.executeQuery();
            if(rs1.next()) {
                startRowid = rs1.getString(1);
                endRowid = rs1.getString(2);
                result.add(startRowid + ":" + endRowid);
            }
        }
        } catch (Exception e){
            LOGGER.error("获取chunk分片 异常：{}",e);
        } finally {
            /*
            if(pso != null){
                pso.close();
            }
            */
            if(ps != null){
                ps.close();
            }
            if(pss != null){
                pss.close();
            }
            if(con != null){
                con.close();
            }
        }
        return result;
    }

    public static List<String> buildSplit(String owner, String table, long blockSize, Connection con) throws Exception {
//        Class.forName("oracle.jdbc.driver.OracleDriver");
//        Connection con=DriverManager.getConnection(
//            "jdbc:oracle:thin:@192.168.239.70:1521:orcl","ant","ant");
        List<String> result = new ArrayList();
        PreparedStatement pso = null;
        PreparedStatement ps= null;
        PreparedStatement pss =null;

        try {
            //pso = con.prepareStatement("select data_object_id from dba_objects t WHERE OWNER = ? AND object_name = ? AND object_type='TABLE'");
            ps = con.prepareStatement("select dbms_rowid.rowid_create(1,?,?,?,0) START_ROWID,dbms_rowid.rowid_create(1,?,?,?,9999) END_ROWID from dual");
            //pss = con.prepareStatement("SELECT file_id,block_id,blocks,relative_fno FROM dba_extents WHERE OWNER =? AND segment_name=? ORDER BY file_id, BLOCK_ID");
            pss = con.prepareStatement("select file_id,block_id,blocks,relative_fno,o.DATA_OBJECT_ID FROM dba_extents e, dba_objects o WHERE o.owner = e.OWNER AND o.object_name = e.SEGMENT_NAME AND e.owner=? AND e.segment_name=? AND nvl(o.subobject_name,0) = nvl(e.partition_name,0) ORDER BY o.DATA_OBJECT_ID,e.file_id, e.BLOCK_ID");
            pss.setObject(1, owner);
            pss.setObject(2, table);
            ResultSet rs = pss.executeQuery();
            /*
            pso.setObject(1, owner);
            pso.setObject(2, table);
            ResultSet rso = pso.executeQuery();
            long objectId = 0;
            if (rso.next()) {
                objectId = rso.getLong(1);
            }
            */
            long chunkBlockSize = 0;
            int bi = 0;
            int ci = 0;
            long b_objectId = 0;
            long b_fileId = 0;
            long b_blockId = 0;
            long b_blocks = 0;
            long b_relativeFno = 0;
            long objectId = 0;
            long fileId = 0;
            long blockId = 0;
            long blocks = 0;
            long relativeFno = 0;
            int pi = 100;
            while (rs.next()) {
                fileId = rs.getInt(1);
                blockId = rs.getInt(2);
                blocks = rs.getInt(3);
                relativeFno = rs.getInt(4);
                objectId = rs.getInt(5);
                if (chunkBlockSize == 0) {
                    b_fileId = fileId;
                    b_blockId = blockId;
                    b_blocks = blocks;
                    b_relativeFno = relativeFno;
                    b_objectId = objectId;
                }
                chunkBlockSize += blocks;
                LOGGER.debug("build:" + (ci++) + "," + chunkBlockSize + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno + "," + objectId);
                if (chunkBlockSize >= blockSize) {
                    LOGGER.debug("handle:" + bi + "," + chunkBlockSize + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno + "," + objectId);
                    ps.setObject(1, b_objectId);
                    ps.setObject(2, b_relativeFno);
                    ps.setObject(3, b_blockId);
                    ps.setObject(4, objectId);
                    ps.setObject(5, relativeFno);
                    ps.setObject(6, blockId + blocks - 1);
                    ResultSet rs1 = ps.executeQuery();
                    rs1.next();
                    String startRowid = rs1.getString(1);
                    String endRowid = rs1.getString(2);
                    LOGGER.debug("rowid:" + startRowid + "," + endRowid);
                    result.add(startRowid + ":" + endRowid);
                    bi++;
                    chunkBlockSize = 0;
                }
            }
            if (chunkBlockSize > 0) {
                LOGGER.debug("handle:" + bi + "," + chunkBlockSize + "," + fileId + "," + blockId + "," + blocks + "," + relativeFno + "," + objectId);
                ps.setObject(1, b_objectId);
                ps.setObject(2, b_relativeFno);
                ps.setObject(3, b_blockId);
                ps.setObject(4, objectId);
                ps.setObject(5, relativeFno);
                ps.setObject(6, blockId + blocks - 1);
                ResultSet rs1 = ps.executeQuery();
                rs1.next();
                String startRowid = rs1.getString(1);
                String endRowid = rs1.getString(2);
                LOGGER.debug("rowid:" + startRowid + "," + endRowid);
                result.add(startRowid + ":" + endRowid);
            }
        } catch (Exception e){
            LOGGER.error("获取chunk分片 异常：{}",e);
        } finally {
            /*
            if(pso != null){
                pso.close();
            }
            */
            if(ps != null){
                ps.close();
            }
            if(pss != null){
                pss.close();
            }
            if(con != null){
                con.close();
            }
        }
        return result;

    }



}
