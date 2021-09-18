package com.mcha.bigdata;

import com.mchz.bigdata.discovery.HBasePump;
import com.mchz.bigdata.hbase.HBaseConn;
import com.mchz.bigdata.hbase.HBaseTable;
import com.mchz.bigdata.sql.HBaseDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MT {
	//wbh.stu
	public static void main(String[] args) throws Exception {
		//HBaseConn hbaseConn = new HBaseConn("127.0.0.1:2181"/*"192.168.239.1:2181"*/);
		HBaseConn hbaseConn = new HBaseConn(/*"127.0.0.1:2181"*/"192.168.239.1:2181");
		HBasePump hbasePump = new HBasePump(hbaseConn);
		List<HBaseTable> allTables = hbasePump.tables();
		List<String> tables = new ArrayList<String>();
		for (HBaseTable ob : allTables) {
			System.out.println("info--->");
			System.out.println(ob.getTableName());
			System.out.println(ob.getNamespace());
			System.out.println("data--->");
			System.out.println(ob.getHbaseColumnList().size());
			if(ob.getHbaseColumnList().size() > 0) {
				//if(ob.getTableName().indexOf("-") < 0) {
					tables.add(ob.getNamespace() + ".\"" + ob.getTableName() + "\"");
				//}
			}
		}
		
		/*
		 * describe 'member'
Table member is ENABLED                                                                                                                                                                                                                                       
member                                                                                                                                                                                                                                                        
COLUMN FAMILIES DESCRIPTION                                                                                                                                                                                                                                   
{NAME => 'address', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLI
CATION_SCOPE => '0'}                                                                                                                                                                                                                                          
{NAME => 'id', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATIO
N_SCOPE => '0'}                                                                                                                                                                                                                                               
{NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICAT
ION_SCOPE => '0'}

create 'member','id','address','info'
0 row(s) in 1.3240 seconds

=> Hbase::Table - member
hbase(main):003:0> put 'member', 'debugo','id','11'
0 row(s) in 0.2300 seconds

hbase(main):004:0> put 'member', 'debugo','info:age','27'
0 row(s) in 0.0070 seconds

hbase(main):005:0> put 'member', 'debugo','info:birthday','1987-04-04'
0 row(s) in 0.0030 seconds

hbase(main):006:0> put 'member', 'debugo','info:industry', 'it'
0 row(s) in 0.0030 seconds

hbase(main):007:0> put 'member', 'debugo','address:city','beijing'
0 row(s) in 0.0170 seconds

hbase(main):008:0> put 'member', 'debugo','address:country','china'
0 row(s) in 0.0030 seconds

hbase(main):009:0> put 'member', 'Sariel', 'id', '21'
0 row(s) in 0.0030 seconds

hbase(main):010:0> put 'member', 'Sariel','info:age', '26'
0 row(s) in 0.0030 seconds

hbase(main):011:0> put 'member', 'Sariel','info:birthday', '1988-05-09 '
0 row(s) in 0.0040 seconds

hbase(main):012:0> put 'member', 'Sariel','info:industry', 'it'
0 row(s) in 0.0040 seconds

hbase(main):013:0> put 'member', 'Sariel','address:city', 'beijing'
0 row(s) in 0.0050 seconds

hbase(main):014:0> put 'member', 'Sariel','address:country', 'china'
0 row(s) in 0.0050 seconds

hbase(main):015:0> put 'member', 'Elvis', 'id', '22'
0 row(s) in 0.0040 seconds

hbase(main):016:0> put 'member', 'Elvis','info:age', '26'
0 row(s) in 0.0030 seconds

hbase(main):017:0> put 'member', 'Elvis','info:birthday', '1988-09-14 '
0 row(s) in 0.0040 seconds

hbase(main):018:0> put 'member', 'Elvis','info:industry', 'it'
0 row(s) in 0.0060 seconds

hbase(main):019:0> put 'member', 'Elvis','address:city', 'beijing'
0 row(s) in 0.0040 seconds

hbase(main):020:0> put 'member', 'Elvis','address:country', 'china'
0 row(s) in 0.0060 seconds

		 * */
		//Student
		//default
		//userinfo
		//autotest_out
		//*
		//HBaseDataSource dataSource = new HBaseDataSource(hbasePump, "default:member", "default:membertest");
		//HBaseDataSource dataSource = new HBaseDataSource(hbasePump, "autotest_out:userinfo");
		//tables.add("default:bigTab");
		//tables.add("chail:userinfo");
		/*
		SELECT * from default.bigTab
		SELECT * from chail.userinfo
		SELECT * from chail.userinfo2
		SELECT * from default.ddd
		SELECT * from default.haha1
		SELECT * from ltest.t1
		SELECT * from ltest.t2
		SELECT * from ltest.userinfo	
		*/	
		
		HBaseDataSource dataSource = new HBaseDataSource(hbasePump,  (String[]) tables.toArray(new String[tables.size()]));

		try (Connection connection = dataSource.getConnection();
				){
		
		for(int ti=0; ti<tables.size();ti++) {
			String sql = "SELECT * from " + tables.get(ti)/*.replace(":", ".")*/;
			System.out.println(sql);
			//String sql = "SELECT * from default.member";
	
			try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql);) {
				/*
				ResultSet trs = connection.getMetaData().getTables(null, null, "MEMBER", null);
				
				while (trs.next()) {
					//System.out.println(trs);
			        String tableName = trs.getString(3);
			        System.out.println(tableName);				
					//System.out.println(rs.getString("id:") + "," + rs.getString("address:city"));
				}
				*/
				ResultSetMetaData rsmd = rs.getMetaData();
				for(int i=1; i<rsmd.getColumnCount()+1; i++) {
					System.out.println(rsmd.getColumnName(i));
					System.out.println(rsmd.getColumnTypeName(i));
					System.out.println(rsmd.getColumnClassName(i));
				}
				/*
				while (rs.next()) {
					System.out.println(rs.getString("a1:EMAIL") + "," + rs.getString("a1:CARDNO"));
					//System.out.println(rs.getString("id:") + "," + rs.getString("address:city"));
				}
				*/
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//*/
		
	}

}
