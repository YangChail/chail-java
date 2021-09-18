
package com.mchz.bigdata.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {
	public static final String CORE_SITE_FILE_NAME = "core-site.xml";
	public static final String HDFS_SITE_NAME = "hdfs-site.xml";
	public static final String YARN_SITE_NAME = "yarn-site.xml";
	public static final String MAPRED_SITE_NAME = "mapred-site.xml";
	public static final String KRB5_FILE_NAME = "krb5.conf";
	public static final String HBASE_SITE_FILE_NAME = "hbase-site.xml";
	
	
	private HBaseConn hbaseConn;
	private int scanCash = 100;
	private int getColumnValueSize = 100;
	public static final String ROW_KEY_NAME="rowkey";

	public HBaseUtil(HBaseConn hbaseConn) {
		this.hbaseConn = hbaseConn;
	}

    public HBaseUtil() {
    }

    public Table getTable(String tableName) throws IOException {
		return hbaseConn.getConnection().getTable(TableName.valueOf(tableName));
	}



	/**
	 * 创建表
	 *
	 * @param tableName 创建表的表名称
	 * @param cfs       列簇的集合
	 * @return
	 */
	public boolean createTable(String tableName, String[] cfs) {
		try (HBaseAdmin admin = hbaseConn.getHbaseAdmin()) {
			if (admin.tableExists(tableName)) {
				return false;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			Arrays.stream(cfs).forEach(cf -> {
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
				columnDescriptor.setMaxVersions(1);
				tableDescriptor.addFamily(columnDescriptor);
			});
			admin.createTable(tableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 获取所有的表格
	 *
	 * @return
	 * @throws IOException
	 */
	public List<HBaseTable> getAllTables() throws Exception {
		List<HBaseTable> tableList = new ArrayList<HBaseTable>();
		HBaseAdmin admin = hbaseConn.getHbaseAdmin();
		HTableDescriptor[] listTables = admin.listTables();
		for (HTableDescriptor htableDescriptor : listTables) {
			HBaseTable hbaseTable = null;
			try {
				TableName tableName = htableDescriptor.getTableName();
				String namespaceAsString = tableName.getNamespaceAsString();
				if(namespaceAsString.equalsIgnoreCase("LQ")) {
					continue;
				}
				String nameAsString = tableName.getNameAsString();
			if (tableName.isSystemTable() || nameAsString.toUpperCase().indexOf("SYSTEM") > -1) {
					continue;
					// 跳过系统表
				}
				boolean tableEnabled = admin.isTableEnabled(tableName);
				if (!tableEnabled) {
					// 跳过关闭的表
					continue;
				}
				Collection<HColumnDescriptor> families = htableDescriptor.getFamilies();
				List<String> columnFamilies = new ArrayList<String>();
				families.forEach(ob -> {
					String nameAsString2 = ob.getNameAsString();
					columnFamilies.add(nameAsString2);
				});

				hbaseTable = new HBaseTable(namespaceAsString, nameAsString, this);
				hbaseTable.setColumnFamilies(columnFamilies);
				// 获取数据
				setTableColumn(hbaseTable);
				if(nameAsString.indexOf(namespaceAsString)>-1) {
					nameAsString=nameAsString.replace(namespaceAsString+":", "");
					hbaseTable.setTableName(nameAsString);
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			tableList.add(hbaseTable);
		}
		return tableList;
	}

	/**
	 * 获取所有namespace
	 *
	 * @return
	 * @throws IOException
	 */
	public List<String> getNamespaceList() throws IOException {
		List<String> nameList = new ArrayList<String>();
		HBaseAdmin admin = hbaseConn.getHbaseAdmin();
		NamespaceDescriptor[] listNamespaceDescriptors = admin.listNamespaceDescriptors();
		for (NamespaceDescriptor NamespaceDescriptor : listNamespaceDescriptors) {
			String name = NamespaceDescriptor.getName();
			nameList.add(name);
		}
		return nameList;
	}

	/**
	 * 获取连接是否有用
	 *
	 * @return
	 */
	public boolean checkConnetionEnbale() {
		return hbaseConn.getHBaseConn().isClosed();
	}

	/**
	 * 关闭连接
	 */
	public void closeConnection() {
		hbaseConn.closeConn();
	}

	/**
	 * 删除表
	 *
	 * @param tableName 表名称
	 * @return
	 */
	public boolean deleteTable(String tableName) {
		try (HBaseAdmin admin = hbaseConn.getHbaseAdmin()) {
			if (!admin.tableExists(tableName)) {
				return false;
			}
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 插入数据
	 *
	 * @param tableName
	 * @param rowkey
	 * @param cfName
	 * @param qualifer
	 * @param data
	 * @return
	 */
	public boolean putRow(String tableName, String rowkey, String cfName, String qualifer, String data) {
		try (Table table = getTable(tableName)) {
			Put put = new Put(Bytes.toBytes(rowkey));
			put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifer), Bytes.toBytes(data));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 批量出入数据
	 *
	 * @param tableName
	 * @param puts
	 * @return
	 */
	public boolean putRows(String tableName, List<Put> puts) {
		try (Table table = getTable(tableName)) {
			table.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 查询单条数据
	 *
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public Result getRow(String tableName, String rowkey) {
		try (Table table = getTable(tableName)) {
			Get get = new Get(Bytes.toBytes(rowkey));
			return table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 带有过滤器的插入数据
	 *
	 * @param tableName
	 * @param rowkey
	 * @param filterList
	 * @return
	 */
	public Result getRow(String tableName, String rowkey, FilterList filterList) {
		try (Table table = getTable(tableName)) {
			Get get = new Get(Bytes.toBytes(rowkey));
			get.setFilter(filterList);
			Result result = table.get(get);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * scan扫描数据，
	 *
	 * @param tableName
	 * @return
	 */
	public ResultScanner getScanner(String tableName, int maxResultSize) {
		try (Table table = getTable(tableName)) {
			Scan scan = new Scan();
			scan.setCaching(scanCash);
			scan.setCacheBlocks(false);
			if(maxResultSize>0) {
				//scan.setFilter(new PageFilter(maxResultSize));
				//scan.setMaxResultSize(maxResultSize);
				//scan.setMaxResultSize(1);
				scan.setAllowPartialResults(true);
				scan.setMaxResultsPerColumnFamily(maxResultSize);
			}
			scan.setBatch(20);
			ResultScanner results = table.getScanner(scan);
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public ResultScanner getScanner(String tableName) {
		return getScanner(tableName, 0);
	}
	
	/**
	 * can 检索数据，控制startrow，stoprow 注意包括startrow 不包括stoprow，
	 *
	 * @param tableName
	 * @param startKey
	 * @param stopKey
	 * @return
	 */
	public ResultScanner getScanner(String tableName, String startKey, String stopKey) {
		try (Table table = getTable(tableName)) {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.setStopRow(Bytes.toBytes(stopKey));
			scan.setCaching(scanCash);
			ResultScanner results = table.getScanner(scan);
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * scan 检索数据，控制startrow，stoprow 注意包括startrow 不包括stoprow，filterList对查询过滤
	 *
	 * @param tableName
	 * @param startKey
	 * @param stopKey
	 * @param filterList
	 * @return
	 */
	public ResultScanner getScanner(String tableName, String startKey, String stopKey, FilterList filterList) {
		try (Table table = getTable(tableName)) {
			Scan scan = new Scan();
			scan.setFilter(filterList);
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.setStopRow(Bytes.toBytes(stopKey));
			scan.setCaching(1000);
			ResultScanner results = table.getScanner(scan);
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 删除行
	 *
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public boolean deleteRow(String tableName, String rowkey) {
		try (Table table = getTable(tableName)) {
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			table.delete(delete);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 删除列簇
	 *
	 * @param tableName
	 * @param cfName
	 * @return
	 */
	public boolean deleteColumnFamily(String tableName, String cfName) {
		try (HBaseAdmin admin = hbaseConn.getHbaseAdmin()) {
			admin.deleteColumn(tableName, cfName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 删除列
	 *
	 * @param tableName
	 * @param cfName
	 * @return
	 */
	public boolean deleteQualifier(String tableName, String rowkey, String cfName, String qualiferName) {
		try (Table table = getTable(tableName)) {
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualiferName));
			table.delete(delete);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 设置字段
	 * @param hbaseTable
	 */
	public void setTableColumn(HBaseTable hbaseTable) {
		String tableName = hbaseTable.getTableName();
		List<String> columnFamilies = hbaseTable.getColumnFamilies();
		ResultScanner scanner = getScanner(tableName, getColumnValueSize);
		Iterator<Result> iterator = scanner.iterator();
		Set<HBaseColumn> colSet = new HashSet<HBaseColumn>();
		List<HBaseColumn> columList=new ArrayList<HBaseColumn>();
		int i = 0;
		while (iterator.hasNext()) {
			Result next = iterator.next();
			if (i > getColumnValueSize) {
				break;
			}
			try {
				int ci = 0;
                for (Cell cell : next.rawCells()) {
                	/*
                    System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
                    System.out.println("Timetamp:"+cell.getTimestamp()+" ");
                    System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
                    System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
                    System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");  
                    */              	
//                    System.out.println(Bytes.toString(cell.getFamilyArray()) + "->" + Bytes.toString(cell.getQualifierArray()) + "->" + Bytes.toString(cell.getValueArray()));                	
					HBaseColumn colum = new HBaseColumn(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneFamily(cell)) );
					colSet.add(colum);                	
					if(ci++ > 20) {
						break;
					}
                }			
				/*
				int ci = 0;
				boolean stop = false;
				for (String str : columnFamilies) {
					if(stop) {
						break;
					}
					byte[] family = Bytes.toBytes(str);
					NavigableMap<byte[], byte[]> familyMap = next.getFamilyMap(family);
					Set<byte[]> keySet = familyMap.keySet();
					//colSet.add(new HbaseColumn(ROW_KEY_NAME,"" ,true)) ;
					for (byte[] key : keySet) {
						String col = Bytes.toString(key);
						HBaseColumn colum = new HBaseColumn(col,str );
						colSet.add(colum);
						ci++;
						if(ci > 20) {
							stop = true;
							break;
						}
					}
				}
				*/
			} catch (Exception e) {
				e.printStackTrace();
			}
			i++;
		}
		if(colSet.size()>0) {
			columList=new ArrayList<HBaseColumn>(colSet);
		}
		hbaseTable.setHbaseColumnList(columList);
	}






	/**
	 * 获取数据
	 * @param hbaseTable
	 * @param size
	 * @return
	 */
	public List<Object[] >  getValue(HBaseTable hbaseTable,int size) {
		String namespace = hbaseTable.getNamespace();
		String tableName = hbaseTable.getTableName();
		if(!namespace.equalsIgnoreCase("default")) {
			tableName=namespace+":"+tableName;
		}
		ResultScanner scanner = getScanner(tableName, size);
		Iterator<Result> iterator = scanner.iterator();
		List<HBaseColumn> hbaseColumnList = hbaseTable.getHbaseColumnList();
		List<Object[] > objList=new ArrayList<Object[]>();
		int count=0;
		while (iterator.hasNext()) {
			if(count>size) {
				break;
			}
			Result next = iterator.next();
			Object[] obj=new Object[hbaseColumnList.size()];
			for(	int i=0;i<hbaseColumnList.size();i++) {
				HBaseColumn ob=hbaseColumnList.get(i);
				String familyName = ob.getFamilyName();
				byte[] family = Bytes.toBytes(familyName);
				NavigableMap<byte[], byte[]> familyMap = next.getFamilyMap(family);
				if(familyMap==null) {
					obj[i]=null;
					continue;
				}
				String name = ob.getName();
				byte[] col = Bytes.toBytes(name);
				byte[] bs = familyMap.get(col);
				if(bs==null) {
					obj[i]=null;
					continue;
				}
				String string = Bytes.toString(bs);
				obj[i]=string;
			}
			objList.add(obj);
			count++;
		}
		return objList;
	}

}
