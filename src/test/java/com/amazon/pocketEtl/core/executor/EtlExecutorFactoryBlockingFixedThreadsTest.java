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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlExecutorFactoryBlockingFixedThreadsTest {
    private final static int NUMBER_OF_WORKERS = 3;
    private final static int QUEUE_SIZE = 5;

    @Mock
    private ThreadPoolExecutor mockThreadPoolExecutor;

    @Mock
    private Runnable mockRunnable;

    @Mock
    private BlockingQueue<Runnable> mockQueue;

    private EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();
    private ThreadPoolExecutor realThreadPoolExecutor;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private EtlExecutor etlExecutor;

    @Before
    public void constructEtlExecutor() {
        etlExecutor = etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(NUMBER_OF_WORKERS, QUEUE_SIZE);
        realThreadPoolExecutor = (ThreadPoolExecutor) ((ExecutorServiceEtlExecutor) etlExecutor).getExecutorService();
        rejectedExecutionHandler = realThreadPoolExecutor.getRejectedExecutionHandler();
    }

    @Before
    public void stubMockThreadPoolExecutor() {
        when(mockThreadPoolExecutor.isShutdown()).thenReturn(false);
        when(mockThreadPoolExecutor.getQueue()).thenReturn(mockQueue);
    }

    @After
    public void teardownEtlExecutor() throws Exception {
        etlExecutor.shutdown();
    }

    @Test
    public void rejectedExecutionPutsThreadOnQueue() throws Exception {
        rejectedExecutionHandler.rejectedExecution(mockRunnable, mockThreadPoolExecutor);

        verify(mockQueue, times(1)).put(mockRunnable);
    }

    @Test(expected = RejectedExecutionException.class)
    public void rejectedExecutionThrowsRejectedExecutionExceptionOnInterruptedExceptionOnQueuePut() throws Exception {
        doThrow(new InterruptedException()).when(mockQueue).put(any(Runnable.class));

        rejectedExecutionHandler.rejectedExecution(mockRunnable, mockThreadPoolExecutor);
    }

    @Test(expected = RejectedExecutionException.class)
    public void rejectedExecutionHandlerThrowsRejectedExecutionExceptionIfThreadPoolExecutorIsShutdown() {
        when(mockThreadPoolExecutor.isShutdown()).thenReturn(true);

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
    public void threadPoolExecutorQueueIsCorrectTypeAndSize() {
        BlockingQueue<Runnable> queue = realThreadPoolExecutor.getQueue();
        assertThat(queue, instanceOf(ArrayBlockingQueue.class));
        assertThat(queue.remainingCapacity(), is(QUEUE_SIZE));
    }

    @Test
    public void executorCanDoRealWork() throws Exception {
        AtomicInteger workCounter = new AtomicInteger(0);

        IntStream.range(0, 100).forEach(i -> etlExecutor.submit(workCounter::incrementAndGet, null));
        etlExecutor.shutdown();

        assertThat(workCounter.get(), equalTo(100));
    }
}
