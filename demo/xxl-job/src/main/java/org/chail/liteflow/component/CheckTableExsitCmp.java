package org.chail.liteflow.component;

import com.yomahub.liteflow.annotation.LiteflowComponent;
import com.yomahub.liteflow.core.NodeCondComponent;
import org.chail.liteflow.slot.DataSlot;

@LiteflowComponent("checkTableExsit")
public class CheckTableExsitCmp extends NodeCondComponent {
    @Override
    public String processCond() throws Exception {
        DataSlot slot = this.getSlot();
        return slot.isTableExist()?"truncateTable":"createTable";
    }
}
