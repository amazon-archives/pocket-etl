/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransformerEtlConsumerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";
    private static final String TEST_STRING1 = "TestStringOne";
    private static final String TEST_STRING2 = "TestStringTwo";
    private static final String TEST_STRING3 = "TestStringThree";

    @Mock
    private EtlConsumer mockEtlConsumer;

    @Mock
    private Transformer<TestDTO, TestDTO> mockTransformer;

    @Mock
    private EtlConsumer mockErrorEtlConsumer;

    private final TestDTO testDTO1 = new TestDTO(TEST_STRING1);
    private final TestDTO testDTO2 = new TestDTO(TEST_STRING2);
    private final TestDTO testDTO3 = new TestDTO(TEST_STRING3);

    private TransformerEtlConsumer<TestDTO, TestDTO> transformerConsumer;

    @Before
    public void constructWorker() {
        transformerConsumer = new TransformerEtlConsumer<>(TEST_NAME, mockEtlConsumer, mockErrorEtlConsumer,
                mockTransformer, TestDTO.class);
    }

    @Test
    public void consumeTransformsAndSendsASingleObjectDownstream() {
        when(mockTransformer.transform(any(TestDTO.class))).thenReturn(ImmutableList.of(testDTO2));
        transformerConsumer.open(mockMetrics);
        transformerConsumer.consume(new EtlStreamObject().with(testDTO1));

        verify(mockTransformer, times(1)).open(any(EtlMetrics.class));
        verify(mockTransformer, times(1)).transform(eq(testDTO1));
        verify(mockEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(testDTO2)));
        verifyNoMoreInteractions(mockTransformer);
    }

    @Test
    public void consumeTransformsAndSendsMultipleObjectsDownstream() {
        when(mockTransformer.transform(any(TestDTO.class))).thenReturn(ImmutableList.of(testDTO2, testDTO3));
        transformerConsumer.open(mockMetrics);
        transformerConsumer.consume(new EtlStreamObject().with(testDTO1));

        verify(mockTransformer, times(1)).open(any(EtlMetrics.class));
        verify(mockTransformer, times(1)).transform(eq(testDTO1));
        verify(mockEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(testDTO2)));
        verify(mockEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(testDTO3)));
        verifyNoMoreInteractions(mockTransformer);
    }

    @Test
    public void consumePassesToTheErrorConsumerOnRuntimeException() {
        when(mockTransformer.transform(any(TestDTO.class))).thenThrow(new RuntimeException("soooo fast"));

        transformerConsumer.open(mockMetrics);
        transformerConsumer.consume(new EtlStreamObject().with(testDTO1));

        verify(mockErrorEtlConsumer).consume(eq(new EtlStreamObject().with(testDTO1)));
    }

    @Test
    public void closeClosesDownstreamWorker() throws Exception {
        transformerConsumer.open(mockMetrics);
        transformerConsumer.close();

        verify(mockEtlConsumer).close();
    }

    @Test
    public void closeClosesDownstreamErrorWorker() throws Exception {
        transformerConsumer.open(mockMetrics);
        transformerConsumer.close();

        verify(mockErrorEtlConsumer).close();
    }

    @Test
    public void closeClosesEverythingWhenDownstreamConsumerThrows() throws Exception {
        doThrow(new RuntimeException("Test Exception")).when(mockEtlConsumer).close();
        transformerConsumer.open(mockMetrics);
        transformerConsumer.close();

        verifyEverythingClosed();
    }

    @Test
    public void closeClosesEverythingWhenTransformerThrows() throws Exception {
        doThrow(new RuntimeException("Test Exception")).when(mockTransformer).close();
        transformerConsumer.open(mockMetrics);
        transformerConsumer.close();

        verifyEverythingClosed();
    }

    @Test
    public void openOpensDownstreamConsumer() {
        transformerConsumer.open(etlProfilingScope.getMetrics());

        verify(mockEtlConsumer).open(eq(etlProfilingScope.getMetrics()));
    }

    @Test
    public void openOpensDownstreamErrorConsumer() {
        transformerConsumer.open(etlProfilingScope.getMetrics());

        verify(mockErrorEtlConsumer).open(eq(etlProfilingScope.getMetrics()));
    }

    private void verifyEverythingClosed() throws Exception {
        verify(mockErrorEtlConsumer).close();
        verify(mockTransformer).close();
        verify(mockEtlConsumer).close();
    }
}
