package org.chail.liteflow.component;

import com.xxl.job.core.context.XxlJobHelper;
import com.yomahub.liteflow.annotation.LiteflowComponent;
import org.slf4j.Logger;

@LiteflowComponent("truncateTable")
public class TruncateTableCmp extends AbstractComponent {


    @Override
    public void process() throws Exception {

        XxlJobHelper.log("正在执行{}",this.getClass().getName());

    }
}
