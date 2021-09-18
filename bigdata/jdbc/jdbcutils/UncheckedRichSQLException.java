package com.chail.apputil.jdbc.jdbcutils;

import java.sql.SQLException;
import java.util.List;


public class UncheckedRichSQLException extends RuntimeException {
	private final RichSQLException exception;

	public UncheckedRichSQLException(SQLException ex, String sql, List<Object> params) {
		this(new RichSQLException(ex, sql, params));
	}

	public UncheckedRichSQLException(RichSQLException e) {
		super(e.getMessage(), e);
		this.exception = e;
	}

	public UncheckedRichSQLException(SQLException e) {
		this(new RichSQLException(e));
	}

	public String getSql() {
		return this.exception.getSql();
	}

	public List<Object> getParams() {
		return this.exception.getParams();
	}

	private static final long serialVersionUID = 1L;
}
