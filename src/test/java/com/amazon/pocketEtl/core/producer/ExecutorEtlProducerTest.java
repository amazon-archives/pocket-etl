/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.exception.GenericEtlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.prefs.BackingStoreException;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ExecutorEtlProducerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";

    @Mock
    private EtlExecutor mockEtlExecutor;

    private List<EtlProducer> mockEtlProducers = new ArrayList<>();
    private ExecutorEtlProducer executorProducer;

    @Before
    public void constructMultiThreadedProducer() {
        IntStream.range(0, 3).forEach(i -> mockEtlProducers.add(mock(EtlProducer.class)));
        executorProducer = new ExecutorEtlProducer(TEST_NAME, mockEtlProducers, mockEtlExecutor);

        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            runnable.run();
            return null;
        }).when(mockEtlExecutor).submit(any(Runnable.class), any(EtlMetrics.class));
    }

    @Test
    public void produceUsesEtlExecutorToProduceAndCloseDownstreamProducers() {
        executorProducer.open(etlProfilingScope.getMetrics());
        executorProducer.produce();

        verify(mockEtlExecutor, times(mockEtlProducers.size())).submit(any(Runnable.class), eq(etlProfilingScope.getMetrics()));

        verifyNormalProducerInteractions();
    }

    @Test
    public void produceClosesProducerAfterException() throws Exception {
        doThrow(new BackingStoreException("Test exception")).when(mockEtlProducers.get(0)).produce();

        executorProducer.open(etlProfilingScope.getMetrics());
        executorProducer.produce();

        verifyNormalProducerInteractions();
    }

    @Test
    public void produceHandlesExceptionOnChildProducerClose() throws Exception {
        doThrow(new Exception("Test exception")).when(mockEtlProducers.get(0)).close();

        executorProducer.open(etlProfilingScope.getMetrics());
        executorProducer.produce();

        verifyNormalProducerInteractions();
    }

    @Test
    public void closeShutsdownEtlExecutor() throws Exception {
        executorProducer.open(mockMetrics);
        executorProducer.close();
        verify(mockEtlExecutor).shutdown();
    }
    
    @Test
    public void closeHandlesGenericEtlExceptionFromExecutor() throws Exception {
        doThrow(new GenericEtlException("Test exception")).when(mockEtlExecutor).shutdown();

        executorProducer.open(mockMetrics);
        executorProducer.close();
    }

    @Test
    public void openOpensAllExtractors() {
        executorProducer.open(etlProfilingScope.getMetrics());

        mockEtlProducers.forEach(mockProducer -> verify(mockProducer, times(1)).open(eq(etlProfilingScope.getMetrics())));
    }

    private void verifyNormalProducerInteractions() {
        mockEtlProducers.forEach(mockProducer -> {
            try {
                InOrder inOrder = inOrder(mockProducer);
                inOrder.verify(mockProducer, times(1)).produce();
                inOrder.verify(mockProducer, times(1)).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
