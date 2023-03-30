package com.chail.demo.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * @author : yangc
 * @date :2023/3/22 17:12
 * @description :
 * @modyified By:
 */
@ActivityInterface
public interface SyncDoAfterActivities {

    @ActivityMethod(name = "after")
    String exec();

}
