/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.pocketEtl.core.executor;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.exception.GenericEtlException;

import java.util.concurrent.RejectedExecutionException;

/**
 * An abstracted interface for an executor that can schedule, queue and perform work.
 */
public interface EtlExecutor {
    /**
     * Complete any remaining work scheduled by the executor and then shut it down.
     *
     * @throws GenericEtlException If something goes wrong.
     */
    void shutdown() throws GenericEtlException;

    /**
     * Queries whether the executor has been shutdown.
     *
     * @return 'true' if the executor has been shutdown, and 'false' if it has not.
     */
    boolean isShutdown();

    /**
     * Schedules a task to be worked on by the Executor. Depending on the implementation this work may be scheduled,
     * worked in parallel with other tasks or block until other work has been completed first. No guarantees are
     * made about when the work will get done.
     *
     * @param task        a runnable wrapping the task to be performed in the future.
     * @param parentMetrics A parent EtlMetrics object to attach the runnables to.
     * @throws RejectedExecutionException If the task cannot be submitted to the Executor.
     */
    void submit(Runnable task, EtlMetrics parentMetrics) throws RejectedExecutionException;
}
