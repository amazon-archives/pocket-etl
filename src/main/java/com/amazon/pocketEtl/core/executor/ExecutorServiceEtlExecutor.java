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
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.exception.GenericEtlException;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An EtlExecutor implementation that wraps an ExecutorService and can be used to parallelize work from a queue. This
 * object should not be constructed directly, instead use EtlExecutorFactory.
 */
@EqualsAndHashCode(exclude = {"executorService"})
class ExecutorServiceEtlExecutor implements EtlExecutor {
    @Getter(AccessLevel.PACKAGE)
    private final ExecutorService executorService;

    ExecutorServiceEtlExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Drain the work-queue and destroy the thread resources used by this object. This request will block until
     * the threads have finished working.
     *
     * @throws GenericEtlException If this thread was interrupted whilst waiting for the shutdown.
     */
    @Override
    public void shutdown() throws GenericEtlException {
        executorService.shutdown();

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ignored) {
        }

        if (!executorService.isTerminated()) {
            executorService.shutdownNow();

            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ignored) {
            }

            if (!executorService.isTerminated()) {
                throw new GenericEtlException("Timed out waiting for forced shutdown of executor service");
            }
        }
    }

    /**
     * Queries whether the executor has been shutdown.
     *
     * @return 'true' if the executor has been shutdown, and 'false' if it has not.
     */
    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    /**
     * Submits a task to the work queue of the Executor. The task will be worked on at some point in the future by
     * one of the threads being managed by the executor unless the executor has been shutdown first. No guarantees are
     * made about when the work will get done.
     *
     * @param task         a runnable wrapping the task to be performed in the future.
     * @param parentMetrics A parent EtlMetrics object to attach the runnables to.
     */
    @Override
    public void submit(Runnable task, EtlMetrics parentMetrics) {
        executorService.submit(() -> {
            try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorServiceEtlExecutor.submit")) {
                task.run();
            }
        });
    }
}
