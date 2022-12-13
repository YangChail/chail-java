package chail.datasupport.tools.model;

import chail.Db2ObjectFiled;

/**
 * @author : yangc
 * @date :2022/6/9 19:38
 * @description :
 * @modyified By:
 */
public class SsTable {

    @Db2ObjectFiled("id")
    private String id;

    @Db2ObjectFiled("name")
    private String name;

    @Db2ObjectFiled("schema_name")
    private String schemaName;

    @Db2ObjectFiled("description")
    private String desc;

    @Db2ObjectFiled("sensitive_source_id")
    private String ssSourceId;


    public String getSsSourceId() {
        return ssSourceId;
    }

    public void setSsSourceId(String ssSourceId) {
        this.ssSourceId = ssSourceId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
