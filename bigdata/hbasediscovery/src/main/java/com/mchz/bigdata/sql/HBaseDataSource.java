package com.mchz.bigdata.sql;

import com.mchz.bigdata.discovery.HBasePump;
import com.mchz.bigdata.hbase.HBaseTable;
import com.mchz.bigdata.hbase.HBaseTableUtil;
import com.mchz.bigdata.hbase.TableRowUtil;
import org.apache.hadoop.hbase.client.Result;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Stream;


public class HBaseDataSource implements DataSource {
	private Connection connection;
	private HBasePump hbasePump;
	private String[] tables;
	
	public HBaseDataSource(HBasePump hbasePump, String... tables) {
		this.hbasePump = hbasePump;
		this.tables = tables;
	}

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection("sa", "sa");
	}

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		if (connection == null) {
			connection = DriverManager.getConnection(
				"jdbc:h2:mem:datapump;db_close_delay=-1;mvcc=true",
				username,
				password);
			for (int i = 0; i < tables.length; i++) {
				populateTable(tables[i]);
			}
		}
		return connection;
	}

	@Deprecated
	private int[] populateTable(String name) throws SQLException {
		return populateTable(row -> row.getNameSpaceAndTableNameSQL().equalsIgnoreCase(name));
	}

	private int[] populateTable(Predicate<HBaseTable> predicate) throws SQLException {
		HBaseTable table = hbasePump.tableMatching(predicate);

		// CREATE SCHEMA ...
		try (PreparedStatement s = connection.prepareStatement(HBaseTableUtil.toSchemaSQL(table));) {
			s.executeUpdate();
		}

		// CREATE TABLE ...
		try (PreparedStatement s = connection.prepareStatement(HBaseTableUtil.toSQL(table));) {
			s.executeUpdate();
		}
		
		// INSERT INTO ...
		try (PreparedStatement s = connection.prepareStatement(HBaseTableUtil.toSQLInsertSyntax(table))) {
			Stream<Result> rowStream = table.rows(100);
			rowStream.forEach(row -> {
				try {
					//row.populateStatement(s);
					TableRowUtil.populateStatement(s, row, table);
					s.addBatch();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
			return s.executeBatch(); 
		}
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException { return null; }

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {}

	@Override
	public int getLoginTimeout() throws SQLException { return 0; }

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException { return null; }

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
