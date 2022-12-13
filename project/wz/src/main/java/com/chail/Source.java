package com.chail;

/**
 * @author : yangc
 * @date :2022/6/1 15:10
 * @description :
 * @modyified By:
 */
public class Source {

    @Db2ObjectFiled("id")
    private String id;

    @Db2ObjectFiled("name")
    private String sourceName;

    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }
}
