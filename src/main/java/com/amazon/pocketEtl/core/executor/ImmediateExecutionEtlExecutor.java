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
import lombok.EqualsAndHashCode;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-threaded implementation of EtlExecutor. Runnables will run in the same thread that submits them.
 */
@EqualsAndHashCode(exclude = {"isShutdown"})
class ImmediateExecutionEtlExecutor implements EtlExecutor {
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Shuts down the EtlExecutor. For this implementation it's a no-op.
     * @throws GenericEtlException If something goes wrong.
     */
    @Override
    public void shutdown() throws GenericEtlException {
        isShutdown.set(true);
    }

    /**
     * Returns the shutdown status of the EtlExecutor.
     * @return true if the EtlExecutor has been shutdown; false if it has not.
     */
    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * Submits a single task to the Executor. This will be executed immediately in the same thread that is calling
     * this method and will not return until the runnable has completed. Any exception thrown by the runnable will
     * be swallowed just like a thread-pool would.
     * @param task         a runnable wrapping the task to be performed in the future.
     * @param parentMetrics A parent EtlMetrics object to attach the runnables to.
     */
    @Override
    public void submit(Runnable task, EtlMetrics parentMetrics) {
        if (isShutdown.get()) {
            throw new RejectedExecutionException("Executor has been shutdown and cannot accept more work");
        }

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SingleThreadedEtlExecutor.submit")) {
            task.run();
        } catch (RuntimeException ignored) {
        }
    }
}
