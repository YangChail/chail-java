package com.chail.demo.activities.impl;

import com.chail.demo.SleepUtils;
import com.chail.demo.activities.SyncDoAfterActivities;

/**
 * @author : yangc
 * @date :2023/3/22 17:16
 * @description :
 * @modyified By:
 */
public class SyncDoAfterActivitiesImpl implements SyncDoAfterActivities {
    @Override
    public String exec() {
        int sleep = SleepUtils.sleep();

        return "success after "+sleep;
    }
}
