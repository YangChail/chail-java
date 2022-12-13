package chail.jdbcutils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;


/**
 * Rich SQL exception.
 *
 */
public class RichSQLException extends Exception {

	private final String sql;
	private final List<Object> params;

	public RichSQLException(SQLException ex, String sql, List<Object> params) {
		super("SQL Exception: " + ex.getMessage() + ":" + sql + "(" + params.toString() + ")", ex);
		this.sql = sql;
		this.params = params;
	}

	public RichSQLException(final SQLException e) {
		this(e, "", Collections.emptyList());
	}

	public String getSql() {
		return sql;
	}

	public List<Object> getParams() {
		return params;
	}

	private static final long serialVersionUID = 1L;

}
