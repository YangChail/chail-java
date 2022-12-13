package com.chail.datasupport.tools.model;

import lombok.Data;

@Data
public class ObjectIncView {
    private String columnId;
    private String columnName;
    private String tableId;
    private String tableName;
    private String schemaId;
    private String schemaName;
    private String insertIncStartValue;
}