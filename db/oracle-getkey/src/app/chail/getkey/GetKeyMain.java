package app.chail.getkey;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetKeyMain {

	private static String address = "";
	private static String username = "";
	private static String password = "";
	private static String tempFile = System.getProperty("user.dir") + File.separator + "connection.properties";
	private static String tableFile = System.getProperty("user.dir") + File.separator + "table.txt";
	private static HashMap<String, String> tableKeyMap = new HashMap<String, String>();
	private static HashMap<String, String> tableMap = new HashMap<String, String>();

	public static void main(String[] args) {
		GetKeyMain getKeyMain = new GetKeyMain();
		getKeyMain.getConproperties();
		getKeyMain.getTable();
		JdbcUtil jdbcUtil = new JdbcUtil(address, username, password);
		for (Map.Entry<String, String> entry : tableMap.entrySet()) {
			String ownerTableName = (String) entry.getKey().toUpperCase();
			System.out.println("seaching..." + ownerTableName);
			if (0 < ownerTableName.indexOf("\\.")) {
				System.out.println(ownerTableName + "  输入错误");
				return;
			}
			String arry[] = ownerTableName.split("\\.");
			String owner = arry[0].toUpperCase();
			String tableName = arry[1].toUpperCase();
			// 查询主键
			List<String> pK = jdbcUtil.excutJDBCUtilQuery("select col.column_name "
					+ "from dba_constraints con,dba_cons_columns col "
					+ "where con.constraint_name=col.constraint_name and con.constraint_type='P' and con.owner=upper('"
					+ owner + "') and col.owner=upper('" + owner + "')and col.table_name=upper('" + tableName + "')");
			if (!pK.isEmpty()) {
				String pkStr="";
				for(String str:pK){
					pkStr+=str+",";
				}
				tableKeyMap.put(ownerTableName, pkStr);
				
				continue;
			}
			// 查询唯一键
			List<String> Uk = jdbcUtil
					.excutJDBCUtilQuery("select col.column_name " + "from dba_constraints con,dba_cons_columns col "
							+ "where con.constraint_name=col.constraint_name and con.constraint_type='U' and con.owner=upper('"
							+ owner + "') and col.table_name=upper('" + tableName + "')");
			if (!Uk.isEmpty()) {
				String strUk = "";
				for (String strUk1 : Uk) {

					strUk += strUk1 + ",";
				}
				tableKeyMap.put(ownerTableName, strUk);
				continue;
			}
			// 查询唯一索引
			List<String> UIndex = jdbcUtil.excutJDBCUtilQuery(
					"select t.COLUMN_NAME from dba_ind_columns t,dba_indexes i where t.index_name = i.index_name and i.uniqueness='UNIQUE' and t.table_name='"
							+ tableName + "' and t.TABLE_OWNER='" + owner + "'");
			if (!UIndex.isEmpty()) {
				String strUIndex = "";
				for (String str1 : UIndex) {
					strUIndex += str1 + ",";
				}
				tableKeyMap.put(ownerTableName, strUIndex);
				continue;
			}
			// 查询索引
			List<String> Index = jdbcUtil.excutJDBCUtilQuery(
					"select c.COLUMN_NAME from dba_indexes i,dba_ind_columns c where i.index_name=c.INDEX_NAME and i.table_name='"
							+ tableName + "'and i.table_owner='" + owner + "'");
			if (!Index.isEmpty()) {
				String strIndex = "";
				for (String str1 : Index) {
					strIndex += str1 + ",";
				}
				tableKeyMap.put(ownerTableName, strIndex);
				continue;
			}
			// 查询所有列
			List<String> all = jdbcUtil.excutJDBCUtilQuery("select COLUMN_NAME from DBA_TAB_COLS where TABLE_NAME = '"
					+ tableName + "' and OWNER='" + owner + "'");
			if (!all.isEmpty()) {
				HashMap<String, String> coltype = jdbcUtil.excutJDBCUtilQuery2(
						"select t.COLUMN_NAME,t.DATA_TYPE from all_tab_columns t where Table_Name='" + tableName
								+ "' and OWNER='" + owner + "'");
				String strall = "";
				for (String str1 : all) {
					String type = coltype.get(str1);
					if (type.equalsIgnoreCase("BLOB") || type.equalsIgnoreCase("clob") || type.equalsIgnoreCase("nclob")
							|| type.equalsIgnoreCase("long") || type.equalsIgnoreCase("long raw")) {
						continue;
					}
					strall += str1 + ",";
				}
				tableKeyMap.put(ownerTableName, strall);
				continue;
			}
			WriteMessegeToLog.writeToLog(ownerTableName, "tableError.log");
		}
		for (Map.Entry<String, String> entry : tableKeyMap.entrySet()) {
			String tablename1 = entry.getKey();
			String str1 = entry.getValue();
			if (str1.endsWith(",")) {
				str1 = str1.substring(0, str1.length() - 1);
			}
			String messege = tablename1 + ";" + str1;
			WriteMessegeToLog.writeToLog(messege, "tableSuccess.log");
		}
		System.out.println("finished...");
	}

	public void getConproperties() {
		Properties properties = new Properties();
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(tempFile));
			try {
				properties.load(in);
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		address = properties.getProperty("address");
		username = properties.getProperty("username");
		password = properties.getProperty("password");
	}

	public void getTable() {
		BufferedReader bre = null;
		String str = "";
		try {
			bre = new BufferedReader(new FileReader(tableFile));
			while ((str = bre.readLine()) != null) // 判断最后一行不存在，为空结束循环
			{
				str = str.trim();
				if (tableMap.get(str) == null) {
					tableMap.put(str, " ");
				}
			}
			;
		} catch (IOException e) {
			e.printStackTrace();
		} // file为文件的路径+文件名称+文件后缀
	}
}
