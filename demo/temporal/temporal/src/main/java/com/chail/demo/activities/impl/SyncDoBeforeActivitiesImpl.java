package com.chail.demo.activities.impl;

import com.chail.demo.SleepUtils;
import com.chail.demo.activities.SyncDoBeforeActivities;

/**
 * @author : yangc
 * @date :2023/3/22 17:15
 * @description :
 * @modyified By:
 */
public class SyncDoBeforeActivitiesImpl implements SyncDoBeforeActivities {
    @Override
    public String exec() {
        int sleep = SleepUtils.sleep();
        return "success defore "+sleep;
    }
}
