package app.chail.getkey;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JdbcUtil {
	/**
	 * log4j
	 */
	/**
	 * 创建数据库连接对象
	 */
	private Connection connnection = null;
	/**
	 * 创建PreparedStatement对象
	 */
	private PreparedStatement preparedStatement = null;
	/**
	 * 创建CallableStatement对象
	 */
	private CallableStatement callableStatement = null;

	private Statement stmt = null;
	/**
	 * 创建结果集对象
	 */
	private ResultSet resultSet = null;

	private String userName;
	private String passwrod;
	private String address;

	static {
		try {
			// 加载数据库驱动程序
			// Class.forName("com.mysql.jdbc.Driver");
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("加载驱动错误");
			System.out.println(e.getMessage());
		}
	}
	
	
	
	public JdbcUtil(String address, String userName,String passwrod) {
		this.userName = userName;
		this.passwrod = passwrod;
		this.address = address;
	}
	
	
	
	/**
	 * 根据Sql查询
	 * 
	 * @author 杨超
	 * @time 2015年8月24日 上午10:18:53
	 */
	public List<String> excutJDBCUtilQuery(String sql) {
		List<String> list = new ArrayList<String>();

		try {
			connnection = DriverManager.getConnection(address, userName,
					passwrod);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			throw new RuntimeException();
		}

		if (connnection != null) {
			try {
				preparedStatement = connnection.prepareStatement(sql);
				preparedStatement.executeQuery();
				resultSet = preparedStatement.getResultSet();
				ResultSetMetaData md = resultSet.getMetaData();
				int columnCount = md.getColumnCount(); // Map rowData;
				while (resultSet.next()) { // rowData = new
					for (int i = 1; i <= columnCount; i++) {
						String re = (String) resultSet.getObject(i);
						list.add(re );
					}
				}
				connnection.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		closeAll();
		return list;
	}
	
	
	/**
	 * 根据Sql查询
	 * 
	 * @author 杨超
	 * @time 2015年8月24日 上午10:18:53
	 */
	public HashMap<String,String> excutJDBCUtilQuery2(String sql) {
		HashMap<String,String> col=new HashMap<String, String>();
		try {
			connnection = DriverManager.getConnection(address, userName,
					passwrod);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			throw new RuntimeException();
		}

		if (connnection != null) {
			try {
				preparedStatement = connnection.prepareStatement(sql);
				ResultSet result =preparedStatement.executeQuery();
				while (result.next()) { // rowData = new
					col.put(result.getObject(1).toString(), result.getObject(2).toString());
				}
				connnection.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		closeAll();
		return col;
	}
	
	

	
	
	private void closeAll() {
		// 关闭结果集对象
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		// 关闭PreparedStatement对象
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		// 关闭CallableStatement 对象
		if (callableStatement != null) {
			try {
				callableStatement.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		// 关闭Connection 对象
		if (connnection != null) {
			try {
				connnection.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) {
		
		JdbcUtil jd=new JdbcUtil("jdbc:oracle:thin:@192.168.200.159:1521:orcl", "etl", "etl");
		String tableName="A";
		String owner="ETL";
		String sql="select * from ID_IN ";
		String sql1="select COLUMN_NAME,DATA_TYPE from all_tab_columns  where Table_Name='"
				+ tableName + "' and OWNER=upper('" + owner + "')";
		
		jd.excutJDBCUtilQuery2(sql1);
	}
	
	
	
}
