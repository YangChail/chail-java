package org.chail.liteflow;

import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.entity.data.LiteflowResponse;
import com.yomahub.liteflow.property.LiteflowConfig;
import org.chail.liteflow.slot.DataSlot;
import org.springframework.stereotype.Component;


@Component
public class LiteflowService {


    public void test(){
        FlowExecutor executor = null;
        if(executor ==null){
            executor = new FlowExecutor();
        }
        LiteflowConfig liteflowConfig = new LiteflowConfig();
        liteflowConfig.setRuleSource("flow.xml");
        executor.setLiteflowConfig(liteflowConfig);
        executor.init();
        LiteflowResponse<DataSlot> response = executor.execute2Resp("mainChain", "arg", DataSlot.class);
    }
}
