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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExecutorEtlConsumerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";

    @Mock
    private EtlStreamObject mockEtlStreamObject;

    @Mock
    private EtlExecutor mockEtlExecutor;

    @Mock
    private EtlConsumer mockEtlConsumer;

    private ExecutorEtlConsumer executorConsumer;

    @Before
    public void constructTransformer() {
        executorConsumer = new ExecutorEtlConsumer(TEST_NAME, mockEtlConsumer, mockEtlExecutor);
    }

    @Before
    public void initializeMockExecutor() {
        when(mockEtlExecutor.isShutdown()).thenReturn(false);
    }

    @Test
    public void consumeSubmitsARunnableThatWritesToConsumer() {
        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            runnable.run();
            return null;
        }).when(mockEtlExecutor).submit(any(Runnable.class), any(EtlMetrics.class));

        executorConsumer.open(etlProfilingScope.getMetrics());
        executorConsumer.consume(mockEtlStreamObject);

        verify(mockEtlExecutor, times(1)).submit(any(Runnable.class), eq(etlProfilingScope.getMetrics()));
        verify(mockEtlConsumer, times(1)).consume(eq(mockEtlStreamObject));
    }

    @Test
    public void closeShutsDownExecutor() throws Exception {
        executorConsumer.open(mockMetrics);
        executorConsumer.close();

        verify(mockEtlExecutor).shutdown();
    }

    @Test
    public void closeClosesConsumer() throws Exception {
        executorConsumer.open(mockMetrics);
        executorConsumer.close();

        verify(mockEtlConsumer).close();
    }

    @Test
    public void openOpensConsumer() {
        executorConsumer.open(etlProfilingScope.getMetrics());

        verify(mockEtlConsumer).open(eq(etlProfilingScope.getMetrics()));
    }


    @Test(expected = IllegalStateException.class)
    public void consumeThrowsIllegalStateExceptionIfExecutorServiceIsShutdown() {
        when(mockEtlExecutor.isShutdown()).thenReturn(true);
        executorConsumer.consume(mockEtlStreamObject);
    }
}
