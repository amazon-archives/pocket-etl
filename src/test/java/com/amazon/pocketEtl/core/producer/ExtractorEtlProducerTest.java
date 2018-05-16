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

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;
import java.util.prefs.BackingStoreException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExtractorEtlProducerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";
    private static final String COLUMN_VALUE1 = "ColumnValue1";
    private static final String COLUMN_VALUE2 = "ColumnValue2";
    private static final String COLUMN_VALUE3 = "ColumnValue3";

    @Mock
    private EtlConsumer mockDownstreamEtlConsumer;

    @Mock
    private Extractor<TestDTO> mockExtractor;

    private ExtractorEtlProducer<TestDTO> extractorProducer;

    @Before
    public void constructExtractorStage() {
        extractorProducer = new ExtractorEtlProducer<>(TEST_NAME, mockDownstreamEtlConsumer, mockExtractor);
        extractorProducer.open(mockMetrics);
    }

    @Test
    public void producePassesExtractedObjectsToDownstreamConsumer() throws Exception {
        when(mockExtractor.next())
                .thenReturn(Optional.of(new TestDTO(COLUMN_VALUE1)))
                .thenReturn(Optional.of(new TestDTO(COLUMN_VALUE2)))
                .thenReturn(Optional.of(new TestDTO(COLUMN_VALUE3)))
                .thenReturn(Optional.empty());

        extractorProducer.produce();

        InOrder inOrder = inOrder(mockDownstreamEtlConsumer);
        inOrder.verify(mockDownstreamEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(new TestDTO(COLUMN_VALUE1))));
        inOrder.verify(mockDownstreamEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(new TestDTO(COLUMN_VALUE2))));
        inOrder.verify(mockDownstreamEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(new TestDTO(COLUMN_VALUE3))));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void produceCanHandleNonFatalExceptions() throws Exception {
        when(mockExtractor.next())
                .thenReturn(Optional.of(new TestDTO(COLUMN_VALUE1)))
                .thenThrow(new RuntimeException("Non-fatal exception"))
                .thenReturn(Optional.of(new TestDTO(COLUMN_VALUE3)))
                .thenReturn(Optional.empty());

        extractorProducer.produce();

        InOrder inOrder = inOrder(mockDownstreamEtlConsumer);
        inOrder.verify(mockDownstreamEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(new TestDTO(COLUMN_VALUE1))));
        inOrder.verify(mockDownstreamEtlConsumer, times(1)).consume(eq(new EtlStreamObject().with(new TestDTO(COLUMN_VALUE3))));
        inOrder.verifyNoMoreInteractions();
    }

    @Test(expected = BackingStoreException.class)
    public void produceThrowsBackingStoreExceptionIfExtractorDoes() throws Exception {
        when(mockExtractor.next()).thenThrow(new BackingStoreException("Test"));

        extractorProducer.produce();
    }

    @Test
    public void produceCanHandleAnEmptyResultSet() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.empty());

        extractorProducer.produce();

        verify(mockDownstreamEtlConsumer, times(1)).open(any(EtlMetrics.class));
        verifyNoMoreInteractions(mockDownstreamEtlConsumer);
    }

    @Test
    public void closeClosesDownstreamConsumer() throws Exception {
        extractorProducer.close();

        verify(mockDownstreamEtlConsumer).close();
    }

    @Test
    public void closeClosesExtractor() throws Exception {
        extractorProducer.close();

        verify(mockExtractor).close();
    }


    @Test(expected = IllegalStateException.class)
    public void produceAfterCloseThrowsIllegalStateException() throws Exception {
        extractorProducer.close();
        extractorProducer.produce();
    }

    @Test(expected = IllegalStateException.class)
    public void produceAfterCloseThrowsIllegalStateExceptionIfConsumerCloseThrowsException() throws Exception {
        doThrow(new RuntimeException()).when(mockDownstreamEtlConsumer).close();

        try {
            extractorProducer.close();
        } catch (RuntimeException ignored) {
        }

        extractorProducer.produce();
    }

    @Test
    public void closeThrowsSameExceptionAsConsumerClose() throws Exception {
        RuntimeException expectedException = new RuntimeException();
        doThrow(expectedException).when(mockDownstreamEtlConsumer).close();
        RuntimeException actualException = null;

        try {
            extractorProducer.close();
        } catch (RuntimeException e) {
            actualException = e;
        }

        assertThat(actualException, is(expectedException));
    }

    @Test
    public void closeSwallowsRuntimeExceptionThrownByExtractor() throws Exception {
        doThrow(new RuntimeException("test exception")).when(mockExtractor).close();
        extractorProducer.close();
    }

    @Test
    public void openOpensDownstreamConsumer() {
        verify(mockDownstreamEtlConsumer, times(1)).open(eq(etlProfilingScope.getMetrics()));
    }

    @Test
    public void produceEmitsAProfilerScope() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.empty());

        extractorProducer.produce();

        verify(mockMetrics).addTime(eq("ExtractorProducer." + TEST_NAME + ".produce"), anyDouble());

    }
}