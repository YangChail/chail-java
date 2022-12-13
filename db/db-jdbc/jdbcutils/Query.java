package com.chail.apputil.jdbc.jdbcutils;

import java.util.Collections;
import java.util.List;

/**
 * This class represents SQL query and parameters.
 */
public class Query {
	private final String sql;
	private final List<Object> params;

	/**
	 * Create new instance.
	 * 
	 * @param sql
	 * @param params
	 */
	public Query(final String sql, final List<Object> params) {
		this.sql = sql;
		this.params = Collections.unmodifiableList(params);
	}

	/**
	 * Get SQL string.
	 * 
	 * @return
	 */
	public String getSQL() {
		return sql;
	}

	/**
	 * Get parameters.
	 * 
	 * @return
	 */
	public List<Object> getParameters() {
		return params;
	}
}
