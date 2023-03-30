package com.chail.demo;

import com.chail.demo.workflow.FullDataSyncWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.WorkerFactory;

import java.util.UUID;

/**
 * @author : yangc
 * @date :2023/3/20 14:15
 * @description :
 * @modyified By:
 */
public class ManagerWorker {
    // Define the task queue name
    static final String FUll_SYNC_QUEUE = "fullDataSyncQueue";

    // Define our workflow unique id
    static final String TABLE_NAME = "fullDataSyncWorkflow";

    public static void main(String[] args) {

        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();

        /*
         * Get a Workflow service client which can be used to start, Signal, and Query Workflow Executions.
         */
        WorkflowClient client = WorkflowClient.newInstance(service);

        /*
         * Define the workflow factory. It is used to create workflow workers for a specific task queue.
         */
        WorkerFactory factory = WorkerFactory.newInstance(client);
        factory.start();
        // Create the workflow client stub. It is used to start our workflow execution.
        FullDataSyncWorkflow workflow =
                client.newWorkflowStub(
                        FullDataSyncWorkflow.class,
                        WorkflowOptions.newBuilder()
                                .setWorkflowId(TABLE_NAME + UUID.randomUUID())
                                .setTaskQueue(FUll_SYNC_QUEUE)
                                .build());

        /*
         * Execute our workflow and wait for it to complete. The call to our getGreeting method is
         * synchronous.
         *
         * See {@link io.temporal.samples.hello.HelloSignal} for an example of starting workflow
         * without waiting synchronously for its result.
         */
        String s = workflow.startSyncFullData(1000L);

        SleepUtils.sleep();
        workflow.stop();


        // Display workflow execution results
        System.out.println(s);

    }
}
