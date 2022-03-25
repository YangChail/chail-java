package org.chail.liteflow.component;


import com.xxl.job.core.context.XxlJobHelper;
import com.yomahub.liteflow.annotation.LiteflowComponent;
import com.yomahub.liteflow.entity.data.Slot;
import org.chail.liteflow.slot.DataSlot;

@LiteflowComponent("init")
public class PreCmp extends AbstractComponent{
    @Override
    public void process() throws Exception {
        DataSlot slot = this.getSlot();









    }
}
