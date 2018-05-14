/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class EtlProducerFactoryTest {
    private static final String PRODUCER_NAME = "producer-name";

    @Mock
    private Extractor<Object> mockExtractor;
    @Mock
    private EtlConsumer mockEtlConsumer;
    @Mock
    private EtlProducer mockEtlProducer;
    @Mock
    private EtlExecutorFactory mockEtlExecutorFactory;

    private EtlProducerFactory etlProducerFactory;

    @Before
    public void initializeEtlProducerFactory() {
        etlProducerFactory = new EtlProducerFactory(mockEtlExecutorFactory);
    }

    @Test
    public void newExtractorProducerReturnsCorrectType() {
        EtlProducer producer = etlProducerFactory.newExtractorProducer(PRODUCER_NAME, mockExtractor, mockEtlConsumer);

        assertThat(producer, instanceOf(ExtractorEtlProducer.class));
    }

    @Test
    public void combineProducersReturnsCorrectType() {
        EtlProducer producer = etlProducerFactory.combineProducers(PRODUCER_NAME, ImmutableList.of(mockEtlProducer), 1);

        assertThat(producer, instanceOf(ExecutorEtlProducer.class));
    }

    @Test
    public void combineProducersUsesCorrectExecutor() {
        etlProducerFactory.combineProducers(PRODUCER_NAME, ImmutableList.of(mockEtlProducer), 5);
        verify(mockEtlExecutorFactory).newUnboundFixedThreadsEtlExecutorFactory(5);
    }

}
