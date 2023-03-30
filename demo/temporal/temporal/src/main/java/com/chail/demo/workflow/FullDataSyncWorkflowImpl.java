package com.chail.demo.workflow;

import com.chail.demo.activities.FullDataSyncActivities;
import com.chail.demo.activities.SyncDoAfterActivities;
import com.chail.demo.activities.SyncDoBeforeActivities;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityOptions;
import io.temporal.samples.hello.HelloActivityExclusiveChoice;
import io.temporal.samples.hello.HelloAsyncActivityCompletion;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;

import java.time.Duration;
import java.util.concurrent.ForkJoinPool;

/**
 * @author : yangc
 * @date :2023/3/22 16:06
 * @description :
 * @modyified By:
 */
@WorkflowInterface
public class FullDataSyncWorkflowImpl implements FullDataSyncWorkflow {
    private boolean stop=false;
    private final SyncDoBeforeActivities syncDoBeforeActivities =
            Workflow.newActivityStub(
                    SyncDoBeforeActivities.class,
                    ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofHours(1)).build());


    private final FullDataSyncActivities fullDataSyncActivities =
            Workflow.newActivityStub(
                    FullDataSyncActivities.class,
                    ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofHours(1)).build());


    private final SyncDoAfterActivities syncDoAfterActivities =
            Workflow.newActivityStub(
                    SyncDoAfterActivities.class,
                    ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofHours(1)).build());


    @Override
    public String startSyncFullData(Long taskId) {
        System.out.println("接受到信号，开始处理,"+taskId);
        syncDoBeforeActivities.exec();

        fullDataSyncActivities.execBuild(taskId+"",taskId);

        fullDataSyncActivities.execDDL(taskId+"",taskId);

        fullDataSyncActivities.execSync(taskId+"",taskId);


        syncDoAfterActivities.exec();
        System.out.println("处理完成");
        return "success";
    }

    @Override
    public void stop() {
        fullDataSyncActivities.stopSync();
    }
}
