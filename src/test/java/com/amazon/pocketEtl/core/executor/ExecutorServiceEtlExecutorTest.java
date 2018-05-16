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


import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.exception.GenericEtlException;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExecutorServiceEtlExecutorTest extends EtlTestBase {
    @Mock
    private ExecutorService mockExecutorService;

    private ExecutorServiceEtlExecutor etlExecutor;

    @Before
    public void constructEtlExecutor() {
        etlExecutor = new ExecutorServiceEtlExecutor(mockExecutorService);
    }

    @Before
    public void configureMockExecutorService() {
        when(mockExecutorService.isShutdown()).thenReturn(false);
        when(mockExecutorService.isTerminated()).thenReturn(false);

    }

    @After
    public void teardownEtlExecutor() {
        try {
            etlExecutor.shutdown();
        } catch (Exception ignored) {

        }
    }

    @Test
    public void submitSubmitsToExecutorService() {
        Runnable mockRunnable = mock(Runnable.class);

        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            runnable.run();
            return null;
        }).when(mockExecutorService).submit(any(Runnable.class));

        etlExecutor.submit(mockRunnable, etlProfilingScope.getMetrics());

        verify(mockRunnable).run();
    }

    @Test
    public void verifyShutdownBehavior() throws Exception {
        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        doAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);
            return null;
        }).when(mockExecutorService).shutdown();

        when(mockExecutorService.shutdownNow()).thenAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);

            when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenAnswer(i2 -> {
                when(mockExecutorService.isTerminated()).thenReturn(true);
                return true;
            });

            return ImmutableList.of();
        });

        etlExecutor.shutdown();

        InOrder inOrder = Mockito.inOrder(mockExecutorService);
        inOrder.verify(mockExecutorService).shutdown();
        inOrder.verify(mockExecutorService).awaitTermination(eq(Long.MAX_VALUE), eq(TimeUnit.NANOSECONDS));
        inOrder.verify(mockExecutorService).shutdownNow();
        inOrder.verify(mockExecutorService).awaitTermination(eq(Long.MAX_VALUE), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void shutdownIgnoresFirstStageInterruptedException() throws Exception {
        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());

        doAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);
            return null;
        }).when(mockExecutorService).shutdown();

        when(mockExecutorService.shutdownNow()).thenAnswer(invocation -> {
            when(mockExecutorService.isTerminated()).thenReturn(true);
            return ImmutableList.of();
        });

        etlExecutor.shutdown();

        InOrder inOrder = Mockito.inOrder(mockExecutorService);
        inOrder.verify(mockExecutorService).shutdown();
        inOrder.verify(mockExecutorService).awaitTermination(eq(Long.MAX_VALUE), eq(TimeUnit.NANOSECONDS));

    }

    @Test
    public void shutdownIgnoresSecondStageInterruptedException() throws Exception {
        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        doAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);
            return null;
        }).when(mockExecutorService).shutdown();

        when(mockExecutorService.shutdownNow()).thenAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);

            when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenAnswer(i2 -> {
                when(mockExecutorService.isTerminated()).thenReturn(true);
                throw new InterruptedException();
            });

            return ImmutableList.of();
        });

        etlExecutor.shutdown();

        InOrder inOrder = Mockito.inOrder(mockExecutorService);
        inOrder.verify(mockExecutorService).shutdown();
        inOrder.verify(mockExecutorService).awaitTermination(eq(Long.MAX_VALUE), eq(TimeUnit.NANOSECONDS));
        inOrder.verify(mockExecutorService).shutdownNow();
        inOrder.verify(mockExecutorService).awaitTermination(eq(Long.MAX_VALUE), eq(TimeUnit.NANOSECONDS));
    }

    @Test(expected = GenericEtlException.class)
    public void shutdownThrowsGenericServiceExceptionIfUnableToShutdown() throws Exception {
        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        doAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);
            return null;
        }).when(mockExecutorService).shutdown();

        when(mockExecutorService.shutdownNow()).thenAnswer(invocation -> {
            when(mockExecutorService.isShutdown()).thenReturn(true);

            when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenAnswer(i2 -> {
                when(mockExecutorService.isTerminated()).thenReturn(false);
                return true;
            });

            return ImmutableList.of(mock(Runnable.class));
        });

        etlExecutor.shutdown();
    }

    @Test
    public void isShutdownReturnsStatusFromExecutorService() {
        assertThat(etlExecutor.isShutdown(), is(false));

        when(mockExecutorService.isShutdown()).thenReturn(true);

        assertThat(etlExecutor.isShutdown(), is(true));
    }
}
