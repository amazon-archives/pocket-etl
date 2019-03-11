/*
 *   Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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
        transformerConsumer.consume(EtlStreamObject.of(testDTO1));

        verify(mockTransformer, times(1)).open(any(EtlMetrics.class));
        verify(mockTransformer, times(1)).transform(eq(testDTO1));
        verify(mockEtlConsumer, times(1)).consume(argThat(
            etlStreamObject -> testDTO2.equals(etlStreamObject.get(TestDTO.class))));
        verifyNoMoreInteractions(mockTransformer);
    }

    @Test
    public void consumeTransformsAndSendsMultipleObjectsDownstream() {
        when(mockTransformer.transform(any(TestDTO.class))).thenReturn(ImmutableList.of(testDTO2, testDTO3));
        transformerConsumer.open(mockMetrics);
        transformerConsumer.consume(EtlStreamObject.of(testDTO1));

        verify(mockTransformer, times(1)).open(any(EtlMetrics.class));
        verify(mockTransformer, times(1)).transform(eq(testDTO1));
        verify(mockEtlConsumer, times(1)).consume(argThat(
            etlStreamObject -> testDTO2.equals(etlStreamObject.get(TestDTO.class))));
        verify(mockEtlConsumer, times(1)).consume(argThat(
            etlStreamObject -> testDTO3.equals(etlStreamObject.get(TestDTO.class))));
        verifyNoMoreInteractions(mockTransformer);
    }

    @Test
    public void consumePassesToTheErrorConsumerOnRuntimeException() {
        when(mockTransformer.transform(any(TestDTO.class))).thenThrow(new RuntimeException("soooo fast"));

        transformerConsumer.open(mockMetrics);
        transformerConsumer.consume(EtlStreamObject.of(testDTO1));

        verify(mockErrorEtlConsumer).consume(argThat(
            etlStreamObject -> testDTO1.equals(etlStreamObject.get(TestDTO.class))));
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
