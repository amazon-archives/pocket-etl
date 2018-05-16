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

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.core.DefaultLoggingStrategy;
import com.amazon.pocketEtl.core.EtlStreamObject;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogAsErrorEtlConsumerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";
    private static final String TEST_LOGGING_STRATEGY_STRING = "TestLoggingStrategyString";

    @Mock
    private Logger mockLogger;

    @Mock
    private EtlStreamObject mockEtlStreamObject;

    @Mock
    private Function<TestDTO, String> mockLoggingStrategy;

    private LogAsErrorEtlConsumer logAsErrorEtlConsumer;

    private LogAsErrorEtlConsumer logAsErrorConsumerWithLogging;

    @Before
    public void constructLogAsErrorConsumerWithLoggingStrategy() {
        logAsErrorConsumerWithLogging = new LogAsErrorEtlConsumer<>(TEST_NAME, mockLogger, TestDTO.class, mockLoggingStrategy);
    }

    @Before
    public void constructLogAsErrorConsumer() {
        logAsErrorEtlConsumer = new LogAsErrorEtlConsumer<>(TEST_NAME, mockLogger, TestDTO.class, new DefaultLoggingStrategy<>());
    }

    @Before
    public void stubLoggingExecute() {
        when(mockLoggingStrategy.apply(any())).thenReturn(TEST_LOGGING_STRATEGY_STRING);
    }

    @Test
    public void consumeDoesNotRevealObjectWithDefaultLoggingStrategy() {
        Object mockLogObject = mock(Object.class);
        when(mockEtlStreamObject.get(any())).thenReturn(mockLogObject);

        logAsErrorEtlConsumer.open(mockMetrics);
        logAsErrorEtlConsumer.consume(mockEtlStreamObject);

        verifyNoMoreInteractions(mockLogObject);
    }

    @Test
    public void consumeLogsErrorWithLoggingStrategy() {
        logAsErrorConsumerWithLogging.open(mockMetrics);
        logAsErrorConsumerWithLogging.consume(mockEtlStreamObject);

        verify(mockLogger).error(endsWith(TEST_LOGGING_STRATEGY_STRING));
    }

    @Test
    public void openDoesNothing() {
        logAsErrorEtlConsumer.open(etlProfilingScope.getMetrics());

        verifyNoMoreInteractions(mockLogger);
    }

    @Test
    public void closeDoesNothing() throws Exception {
        logAsErrorEtlConsumer.open(mockMetrics);
        logAsErrorEtlConsumer.close();

        verifyNoMoreInteractions(mockLogger);
    }

    @Test
    public void consumeStillLogsAnErrorIfStrategyThrowsRuntimeException() {
        RuntimeException e = new RuntimeException("Test exception");

        when(mockLoggingStrategy.apply(any())).thenThrow(e);

        logAsErrorConsumerWithLogging.open(mockMetrics);
        logAsErrorConsumerWithLogging.consume(mockEtlStreamObject);

        verify(mockLogger).error(contains(mockEtlStreamObject.getClass().getSimpleName()), eq(e));
    }
}
