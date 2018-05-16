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
public class EtlTransformStageTest {
    private final static String EXPECTED_DEFAULT_STAGE_NAME = "EtlStream.Transform";
    private final static int EXPECTED_DEFAULT_QUEUE_SIZE = 1000;

    @Mock
    private Transformer<Object, ?> mockTransformer;
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
    @Mock
    private EtlConsumer mockDownstreamConsumer;

    private EtlTransformStage<Object> etlTransformStage;

    @Before
    public void constructEtlLoadStage() {
        etlTransformStage = new EtlTransformStage<>(Object.class, mockTransformer, EXPECTED_DEFAULT_STAGE_NAME, 1, mockObjectLogger,
                mockEtlExecutorFactory, mockEtlConsumerFactory);
    }

    @Test
    public void staticConstructorSetsDefaultProperties() {
        EtlTransformStage<Object> testStage = EtlTransformStage.of(Object.class, mockTransformer);

        assertThat(testStage.getTransformer(), equalTo(mockTransformer));
        assertThat(testStage.getStageName(), equalTo(EXPECTED_DEFAULT_STAGE_NAME));
        assertThat(testStage.getClassForStage(), equalTo(Object.class));
        assertThat(testStage.getNumberOfThreads(), is(1));
        assertThat(testStage.getObjectLogger(), equalTo(new DefaultLoggingStrategy<>()));
    }

    @Test
    public void withStageNameUpdatesProperty() {
        EtlTransformStage<Object> testStage = etlTransformStage.withName("custom-name");

        assertThat(testStage.getStageName(), equalTo("custom-name"));
    }

    @Test
    public void withObjectLoggerUpdatesProperty() {
        EtlTransformStage<Object> testStage = etlTransformStage.withObjectLogger(mockObjectLogger2);

        assertThat(testStage.getObjectLogger(), equalTo(mockObjectLogger2));
    }

    @Test
    public void withThreadsUpdatesProperty() {
        EtlTransformStage<Object> testStage = etlTransformStage.withThreads(3);

        assertThat(testStage.getNumberOfThreads(), is(3));
    }

    @Test
    public void constructConsumerForStageConstructsConsumer() {
        when(mockEtlExecutorFactory.newBlockingFixedThreadsEtlExecutor(anyInt(), anyInt())).thenReturn(mockEtlExecutor);
        when(mockEtlConsumerFactory.newLogAsErrorConsumer(anyString(), any(), any(), any())).thenReturn(mockErrorConsumer);
        when(mockEtlConsumerFactory.newTransformer(anyString(), any(), any(), any(), any(), any())).thenReturn(mockConsumer);

        EtlConsumer result = etlTransformStage.constructConsumerForStage(mockDownstreamConsumer);

        assertThat(result, is(mockConsumer));
        verify(mockEtlExecutorFactory).newBlockingFixedThreadsEtlExecutor(1, EXPECTED_DEFAULT_QUEUE_SIZE);
        verify(mockEtlConsumerFactory).newLogAsErrorConsumer(
                eq(EXPECTED_DEFAULT_STAGE_NAME),
                argThat(logger -> EXPECTED_DEFAULT_STAGE_NAME.equals(logger.getName())),
                eq(Object.class),
                eq(mockObjectLogger));
        verify(mockEtlConsumerFactory).newTransformer(EXPECTED_DEFAULT_STAGE_NAME, mockTransformer, Object.class,
                mockDownstreamConsumer, mockErrorConsumer, mockEtlExecutor);
    }

}