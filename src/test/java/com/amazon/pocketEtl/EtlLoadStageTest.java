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

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.DefaultLoggingStrategy;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.consumer.EtlConsumerFactory;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlLoadStageTest {
    private final static String EXPECTED_DEFAULT_STAGE_NAME = "EtlStream.Load";
    private final static int EXPECTED_DEFAULT_QUEUE_SIZE = 1000;


    @Mock
    private Loader<Object> mockLoader;
    @Mock
    private Function<Object, String> mockObjectLogger;
    @Mock
    private Function<Object, String> mockObjectLogger2;
    @Mock
    private EtlExecutorFactory mockEtlExecutorFactory;
    @Mock
    private EtlConsumerFactory mockEtlConsumerFactory;
    @Mock
    private EtlExecutor mockEtlExecutor;
    @Mock
    private EtlConsumer mockErrorConsumer;
    @Mock
    private EtlConsumer mockConsumer;

    private EtlLoadStage<Object> etlLoadStage;

    @Before
    public void constructEtlLoadStage() {
        etlLoadStage = new EtlLoadStage<>(Object.class, mockLoader, EXPECTED_DEFAULT_STAGE_NAME, 1, mockObjectLogger,
                mockEtlExecutorFactory, mockEtlConsumerFactory);
    }

    @Test
    public void staticConstructorSetsDefaultProperties() {
        EtlLoadStage<Object> testStage = EtlLoadStage.of(Object.class, mockLoader);

        assertThat(testStage.getLoader(), equalTo(mockLoader));
        assertThat(testStage.getStageName(), equalTo(EXPECTED_DEFAULT_STAGE_NAME));
        assertThat(testStage.getClassForStage(), equalTo(Object.class));
        assertThat(testStage.getNumberOfThreads(), is(1));
        assertThat(testStage.getObjectLogger(), equalTo(new DefaultLoggingStrategy<>()));
    }

    @Test
    public void withStageNameUpdatesProperty() {
        EtlLoadStage<Object> testStage = etlLoadStage.withName("custom-name");

        assertThat(testStage.getStageName(), equalTo("custom-name"));
    }

    @Test
    public void withObjectLoggerUpdatesProperty() {
        EtlLoadStage<Object> testStage = etlLoadStage.withObjectLogger(mockObjectLogger2);

        assertThat(testStage.getObjectLogger(), equalTo(mockObjectLogger2));
    }

    @Test
    public void withThreadsUpdatesProperty() {
        EtlLoadStage<Object> testStage = etlLoadStage.withThreads(3);

        assertThat(testStage.getNumberOfThreads(), is(3));
    }

    @Test
    public void constructConsumerForStageConstructsConsumer() {
        when(mockEtlExecutorFactory.newBlockingFixedThreadsEtlExecutor(anyInt(), anyInt())).thenReturn(mockEtlExecutor);
        when(mockEtlConsumerFactory.newLogAsErrorConsumer(anyString(), any(), any(), any())).thenReturn(mockErrorConsumer);
        when(mockEtlConsumerFactory.newLoader(anyString(), any(), any(), any(), any())).thenReturn(mockConsumer);

        EtlConsumer result = etlLoadStage.constructConsumerForStage(null);

        assertThat(result, is(mockConsumer));
        verify(mockEtlExecutorFactory).newBlockingFixedThreadsEtlExecutor(1, EXPECTED_DEFAULT_QUEUE_SIZE);
        verify(mockEtlConsumerFactory).newLogAsErrorConsumer(
                eq(EXPECTED_DEFAULT_STAGE_NAME),
                argThat(logger -> EXPECTED_DEFAULT_STAGE_NAME.equals(logger.getName())),
                eq(Object.class),
                eq(mockObjectLogger));
        verify(mockEtlConsumerFactory).newLoader(EXPECTED_DEFAULT_STAGE_NAME, mockLoader, Object.class,
                mockErrorConsumer, mockEtlExecutor);
    }
}