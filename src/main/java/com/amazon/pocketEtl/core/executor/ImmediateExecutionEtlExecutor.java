/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
