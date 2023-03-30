package com.chail.demo;

import com.chail.demo.activities.impl.FullDataSyncActivitiesImpl;
import com.chail.demo.activities.impl.SyncDoAfterActivitiesImpl;
import com.chail.demo.activities.impl.SyncDoBeforeActivitiesImpl;
import com.chail.demo.workflow.FullDataSyncWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * @author : yangc
 * @date :2023/3/22 16:20
 * @description :
 * @modyified By:
 */
public class EngineWorker {

    // Define the task queue name
    static final String TASK_QUEUE = "fullDataSyncQueue";

    // Define our workflow unique id
    static final String WORKFLOW_ID = "fullDataSyncWorkflow";
    public static void main(String[] args) {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(FullDataSyncWorkflowImpl.class);
        worker.registerActivitiesImplementations
                (new SyncDoBeforeActivitiesImpl(),new FullDataSyncActivitiesImpl(),new SyncDoAfterActivitiesImpl());

        //开始
        factory.start();
        while (true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
