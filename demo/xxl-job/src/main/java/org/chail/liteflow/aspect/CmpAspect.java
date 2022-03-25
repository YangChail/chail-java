package org.chail.liteflow.aspect;

import com.xxl.job.core.context.XxlJobHelper;
import com.yomahub.liteflow.aop.ICmpAroundAspect;
import com.yomahub.liteflow.entity.data.Slot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmpAspect implements ICmpAroundAspect {
    private static Logger logger = LoggerFactory.getLogger(CmpAspect.class);

    @Override
    public void beforeProcess(String nodeId, Slot slot) {
        XxlJobHelper.log("正在开始执行{}",nodeId);
    }

    @Override
    public void afterProcess(String nodeId, Slot slot) {
        XxlJobHelper.log("正在开始执行{}",nodeId);
    }
}
