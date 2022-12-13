package com.chail.datasupport.tools.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/9 10:18
 * @description :
 * @modyified By:
 */
public class SqlObject {

   private  List<Integer> sensitiveIds;
   private  List<Integer> sourceIds;
   private  String sql;

    public List<Integer> getSensitiveIds() {
        return sensitiveIds;
    }

    public void setSensitiveIds(List<Integer> sensitiveIds) {
        this.sensitiveIds = sensitiveIds;
    }

    public List<Integer> getSourceIds() {
        return sourceIds;
    }

    public void setSourceIds(List<Integer> sourceIds) {
        this.sourceIds = sourceIds;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
