package com.chail.demo.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.workflow.SignalMethod;

@ActivityInterface
public interface FullDataSyncActivities {
    @ActivityMethod(name = "build")
    String execBuild(String parm, Long taskId);
    @ActivityMethod(name = "ddl")
    String execDDL(String parm,  Long taskId);
    @ActivityMethod(name = "sync")
    String execSync(String parm,  Long taskId);

    @SignalMethod(name = "stop-signal")
    void stopSync();
}
