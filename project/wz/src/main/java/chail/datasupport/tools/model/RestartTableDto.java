package chail.datasupport.tools.model;

import java.util.List;

public class RestartTableDto {

    /**
     *  是否重新创建表结构
     */
    private Boolean rebuildTable = true;

    /**
     * 表格id列表
     */
    private List<Long> ids;

    private Boolean fromTotal = false;

    /**
     * 增量冗余处理的时间
     */
    private List<Long> incStartTime;

    public Boolean isRebuildTable() {
        return rebuildTable;
    }

    public void setRebuildTable(Boolean rebuildTable) {
        this.rebuildTable = rebuildTable;
    }

    public List<Long> getIds() {
        return ids;
    }

    public void setIds(List<Long> ids) {
        this.ids = ids;
    }

    public Boolean getFromTotal() {
        return fromTotal;
    }

    public void setFromTotal(Boolean fromTotal) {
        this.fromTotal = fromTotal;
    }


    public List<Long> getIncStartTime() {
        return incStartTime;
    }

    public void setIncStartTime(List<Long> incStartTime) {
        this.incStartTime = incStartTime;
    }
}