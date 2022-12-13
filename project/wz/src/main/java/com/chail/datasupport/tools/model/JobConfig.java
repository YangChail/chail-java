package com.chail.datasupport.tools.model;

import com.chail.Db2ObjectFiled;

import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/9 10:20
 * @description :
 * @modyified By:
 */
public class JobConfig {

    @Db2ObjectFiled("id")
    private String id;
    @Db2ObjectFiled("job_id")
    private String job_id;
    @Db2ObjectFiled("name")
    private String name;
    @Db2ObjectFiled("value")
    private String value;

    private List<SqlObject> sqlObjects;


    public List<SqlObject> getSqlObjects() {
        return sqlObjects;
    }

    public void setSqlObjects(List<SqlObject> sqlObjects) {
        this.sqlObjects = sqlObjects;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJob_id() {
        return job_id;
    }

    public void setJob_id(String job_id) {
        this.job_id = job_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
