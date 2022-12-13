package chail.datasupport.tools.model;

/**
 * @author : yangc
 * @date :2022/6/9 19:15
 * @description :
 * @modyified By:
 */
public class V2SqView {

    private String jobId;

    private String tableName;

    private String sql;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
