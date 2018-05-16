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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An injectable factory class for building various types of useful EtlExecutor implementations. This is the only way
 * you should be constructing EtlExecutor objects of any kind outside of this package.
 */
public class EtlExecutorFactory {
    /**
     * This multi-threaded EtlExecutor uses a fixed-size work queue that will block on new requests once the queue is
     * full until it drains enough to add the new task.
     * @param numberOfWorkers Number of threads to run tasks simultaneously.
     * @param queueSize The maximum size of the work-queue. Submit will block once this hits its size limit.
     * @return A fully constructed EtlExecutor.
     */
    public EtlExecutor newBlockingFixedThreadsEtlExecutor(int numberOfWorkers, int queueSize) {
        ExecutorService executorService = new ThreadPoolExecutor(numberOfWorkers, numberOfWorkers, Long.MAX_VALUE,
                TimeUnit.NANOSECONDS, new ArrayBlockingQueue<>(queueSize), (runnable, threadPoolExecutor) -> {
                    if (threadPoolExecutor.isShutdown()) {
                        throw new RejectedExecutionException("ExecutorService was shutdown");
                    }
                    try {
                        threadPoolExecutor.getQueue().put(runnable);
                    } catch (InterruptedException ignored) {
                        throw new RejectedExecutionException("Thread was interrupted trying to queue new work");
                    }
                });

        return new ExecutorServiceEtlExecutor(executorService);
    }

    /**
     * This multi-threaded EtlExecutor uses an unbound queue that will not block on new requests.
     * @param numberOfWorkers Number of threads to run tasks simultaneously.
     * @return A fully constructed EtlExecutor.
     */
    public EtlExecutor newUnboundFixedThreadsEtlExecutorFactory(int numberOfWorkers) {
        return new ExecutorServiceEtlExecutor(Executors.newFixedThreadPool(numberOfWorkers));
    }

    /**
     * This is a single-threaded EtlExecutor used when you don't want any kind of parallelism, but conceptually will
     * behave like other EtlExecutors. Executions will block until completed by the invoking thread. This should be
     * used with caution because it does not protect downstream consumers against unwanted parallelism.
     * @return A fully constructed EtlExecutor.
     */
    public EtlExecutor newImmediateExecutionEtlExecutor() {
        return new ImmediateExecutionEtlExecutor();
    }
}
