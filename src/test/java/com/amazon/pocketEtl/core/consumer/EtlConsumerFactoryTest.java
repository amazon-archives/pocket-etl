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

import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.core.DefaultLoggingStrategy;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlConsumerFactoryTest {
    private static final String STAGE_NAME = "test-stage";

    @Mock
    private Loader<Object> mockLoader;
    @Mock
    private Transformer<Object, Object> mockTransformer;
    @Mock
    private EtlConsumer mockErrorConsumer;
    @Mock
    private EtlConsumer mockDownstreamConsumer;
    @Mock
    private EtlExecutor mockEtlExecutor;
    @Mock
    private Logger mockLogger;
    @Mock
    private EtlExecutorFactory mockEtlExecutorFactory;

    private EtlConsumerFactory etlConsumerFactory;

    @Before
    public void initializeConsumerFactory() {
        etlConsumerFactory = new EtlConsumerFactory(mockEtlExecutorFactory);
        when(mockEtlExecutorFactory.newImmediateExecutionEtlExecutor()).thenReturn(mockEtlExecutor);
    }

    @Test
    public void newLoaderCreatesAWrappedLoaderConsumer() {
        EtlConsumer consumer = etlConsumerFactory.newLoader(STAGE_NAME, mockLoader, Object.class, mockErrorConsumer, mockEtlExecutor);
        verifyWrappedConsumerStack(consumer, LoaderEtlConsumer.class);
    }

    @Test
    public void newTransformerCreatesAWrappedTransformerConsumer() {
        EtlConsumer consumer = etlConsumerFactory.newTransformer(STAGE_NAME, mockTransformer, Object.class, mockDownstreamConsumer,
                mockErrorConsumer, mockEtlExecutor);

        verifyWrappedConsumerStack(consumer, TransformerEtlConsumer.class);
    }

    @Test
    public void newLogAsErrorCreatesAWrappedLogAsErrorConsumer() {
        EtlConsumer consumer = etlConsumerFactory.newLogAsErrorConsumer(STAGE_NAME, mockLogger, Object.class, new DefaultLoggingStrategy<>());

        verifyWrappedConsumerStack(consumer, LogAsErrorEtlConsumer.class);
    }

    private void verifyWrappedConsumerStack(EtlConsumer consumer, Class expectedClass) {
        assertThat(consumer, instanceOf(SmartEtlConsumer.class));

        consumer = ((SmartEtlConsumer)consumer).getWrappedEtlConsumer();
        assertThat(consumer, instanceOf(MetricsEmissionEtlConsumer.class));

        consumer = ((MetricsEmissionEtlConsumer)consumer).getDownstreamEtlConsumer();
        assertThat(consumer, instanceOf(ExecutorEtlConsumer.class));

        consumer = ((ExecutorEtlConsumer)consumer).getWrappedEtlConsumer();
        assertThat(consumer, instanceOf(expectedClass));
    }
}
