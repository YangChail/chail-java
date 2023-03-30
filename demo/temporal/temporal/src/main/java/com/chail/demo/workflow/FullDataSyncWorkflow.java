package com.chail.demo.workflow;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * @author : yangc
 * @date :2023/3/22 16:07
 * @description :
 * @modyified By:
 */
@WorkflowInterface
public interface FullDataSyncWorkflow {

    @WorkflowMethod
    String startSyncFullData(Long taskId);


    @SignalMethod(name = "stop-workflow-signal")
    void stop();

}
