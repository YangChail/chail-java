package org.chail.controller;


import com.alibaba.fastjson.JSON;
import com.yomahub.liteflow.core.FlowExecutor;
import com.yomahub.liteflow.entity.data.DefaultSlot;
import com.yomahub.liteflow.entity.data.LiteflowResponse;
import com.yomahub.liteflow.property.LiteflowConfig;
import org.chail.liteflow.slot.DataSlot;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class TestController {

    private  FlowExecutor executor;
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String index(ModelMap modelMap){

        if(executor ==null){
            executor = new FlowExecutor();
        }

        LiteflowConfig liteflowConfig = new LiteflowConfig();
        liteflowConfig.setRuleSource("flow.xml");
        executor.setLiteflowConfig(liteflowConfig);
        executor.init();
        LiteflowResponse<DataSlot> response = executor.execute2Resp("mainChain", "arg", DataSlot.class);

        return "index";
    }


}
