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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class EtlExecutorFactoryUnboundFixedThreadsTest {
    private final static int NUMBER_OF_WORKERS = 3;

    @Mock
    private ThreadPoolExecutor mockThreadPoolExecutor;

    @Mock
    private Runnable mockRunnable;

    private EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();
    private ThreadPoolExecutor realThreadPoolExecutor;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private EtlExecutor etlExecutor;

    @Before
    public void constructEtlExecutor() {
        etlExecutor = etlExecutorFactory.newUnboundFixedThreadsEtlExecutorFactory(NUMBER_OF_WORKERS);
        realThreadPoolExecutor = (ThreadPoolExecutor) ((ExecutorServiceEtlExecutor) etlExecutor).getExecutorService();
        rejectedExecutionHandler = realThreadPoolExecutor.getRejectedExecutionHandler();
    }

    @After
    public void teardownEtlExecutor() throws Exception {
        etlExecutor.shutdown();
    }

    @Test(expected = RejectedExecutionException.class)
    public void rejectedExecutionThrowsRejectedExecutionException() {
        rejectedExecutionHandler.rejectedExecution(mockRunnable, mockThreadPoolExecutor);
    }

    @Test
    public void threadPoolExecutorHasCorrectCorePoolSize() {
        assertThat(realThreadPoolExecutor.getCorePoolSize(), is(NUMBER_OF_WORKERS));
    }

    @Test
    public void threadPoolExecutorHasCorrectMaxPoolSize() {
        assertThat(realThreadPoolExecutor.getMaximumPoolSize(), is(NUMBER_OF_WORKERS));
    }

    @Test
    public void threadPoolExecutorQueueIsCorrectType() {
        BlockingQueue<Runnable> queue = realThreadPoolExecutor.getQueue();
        assertThat(queue, instanceOf(LinkedBlockingQueue.class));
    }

    @Test
    public void executorCanDoRealWork() throws Exception {
        AtomicInteger workCounter = new AtomicInteger(0);

        IntStream.range(0, 100).forEach(i -> etlExecutor.submit(workCounter::incrementAndGet, null));
        etlExecutor.shutdown();

        assertThat(workCounter.get(), equalTo(100));
    }
}
