package com.chail.demo.activities.impl;

import com.chail.demo.activities.FullDataSyncActivities;
import com.chail.demo.SleepUtils;

/**
 * @author : yangc
 * @date :2023/3/22 16:15
 * @description :
 * @modyified By:
 */
public class FullDataSyncActivitiesImpl implements FullDataSyncActivities {

    private boolean stop=false;

    @Override
    public String execBuild(String parm, Long taskId) {
        int sleep = SleepUtils.sleep();
        return "build success"+"sleep "+sleep+"S";
    }

    @Override
    public String execDDL(String parm, Long taskId) {
        int sleep = SleepUtils.sleep();
        return "ddl success"+"sleep "+sleep+"S";
    }

    @Override
    public String execSync(String parm, Long taskId) {
        int sleep = SleepUtils.sleep();

        while (!stop){
            System.out.println("syncing...");
            SleepUtils.sleep();
        }

        return "sync success"+"sleep "+sleep+"S";
    }

    @Override
    public void stopSync() {
        stop=true;
    }


}
