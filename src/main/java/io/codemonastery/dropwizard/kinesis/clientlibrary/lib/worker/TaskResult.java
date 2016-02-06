/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker;

/**
 * Used to capture information from a task that we want to communicate back to the higher layer.
 * E.g. exception thrown when executing the task, if we reach end of a shard.
 */
class TaskResult {

    // Did we reach the end of the shard while processing this task.
    private boolean shardEndReached;

    // Any exception caught while executing the task.
    private Exception exception;

    /**
     * @return the shardEndReached
     */
    protected boolean isShardEndReached() {
        return shardEndReached;
    }

    /**
     * @param shardEndReached the shardEndReached to set
     */
    protected void setShardEndReached(boolean shardEndReached) {
        this.shardEndReached = shardEndReached;
    }

    /**
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    /**
     * @param e Any exception encountered when running the process task.
     */
    TaskResult(Exception e) {
        this(e, false);
    }

    /**
     * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
     */
    TaskResult(boolean isShardEndReached) {
        this(null, isShardEndReached);
    }

    /**
     * @param e Any exception encountered when executing task.
     * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
     */
    TaskResult(Exception e, boolean isShardEndReached) {
        this.exception = e;
        this.shardEndReached = isShardEndReached;
    }

}
