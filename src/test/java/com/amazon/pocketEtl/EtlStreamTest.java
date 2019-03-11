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

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.DefaultLoggingStrategy;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.consumer.EtlConsumerFactory;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.amazon.pocketEtl.core.producer.EtlProducerFactory;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static com.amazon.pocketEtl.EtlConsumerStage.load;
import static com.amazon.pocketEtl.EtlConsumerStage.transform;
import static com.amazon.pocketEtl.EtlProducerStage.extract;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlStreamTest {

    @Mock
    private EtlRunner mockEtlRunner;
    @Mock
    private Extractor<SimpleBeanClass> mockExtractor;
    @Mock
    private Extractor<SimpleBeanClass> mockExtractor2;
    @Mock
    private Extractor<SimpleBeanClass> mockExtractor3;
    @Mock
    private Transformer<SimpleBeanClass, SimpleBeanClass> mockTransformer;
    @Mock
    private Transformer<SimpleBeanClass, SimpleBeanClass> mockTransformer2;
    @Mock
    private Loader<SimpleBeanClass> mockLoader;
    @Mock
    private Loader<SimpleBeanClass> mockLoader2;
    @Mock
    private EtlMetrics mockMetrics;

    private final EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();
    private final EtlConsumerFactory etlConsumerFactory = new EtlConsumerFactory(etlExecutorFactory);
    private final EtlProducerFactory etlProducerFactory = new EtlProducerFactory(etlExecutorFactory);
    private final EtlExecutor defaultExecutor = etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(1, 1000);

    private SimpleBeanClass simpleObject = new SimpleBeanClass("Test");

    @Captor
    private ArgumentCaptor<EtlProducer> etlProducerArgumentCaptor;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class SimpleBeanClass {
        private String field;
    }

    @Before
    public void initializeMockEtlComponents() {
        when(mockExtractor.next()).thenReturn(Optional.of(simpleObject)).thenReturn(Optional.empty());
        when(mockTransformer.transform(any())).thenReturn(ImmutableList.of(simpleObject));
    }

    private EtlProducer getProducerFromRunner() throws Exception {
        verify(mockEtlRunner, times(1)).run(etlProducerArgumentCaptor.capture(), eq(mockMetrics));
        return etlProducerArgumentCaptor.getValue();
    }

    @Test
    public void canConstructASimpleEtlStream() throws Exception {
        EtlStream.extract(mockExtractor)
                .transform(SimpleBeanClass.class, mockTransformer)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test(expected = IllegalArgumentException.class)
    public void combiningOneStreamThrowsIllegalArgumentException() {
        EtlStream.combine(EtlStream.extract(mockExtractor));
    }

    @Test(expected = IllegalArgumentException.class)
    public void combiningNoStreamsThrowsIllegalArgumentException() {
        EtlStream.combine();
    }

    @Test
    public void combinedExtractorsAggregateThreadsCorrectly() throws Exception {
        EtlStream stream1 = EtlStream.from(extract(mockExtractor).withName("extract1"));
        EtlStream stream2 = EtlStream.from(extract(mockExtractor2, mockExtractor3).withName("extract2"));

        EtlStream.combine(stream1, stream2)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer1 = etlProducerFactory.newExtractorProducer("extract1", mockExtractor,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer2 = etlProducerFactory.newExtractorProducer("extract2", mockExtractor2,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer3 = etlProducerFactory.newExtractorProducer("extract2", mockExtractor3,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer = etlProducerFactory.combineProducers("EtlStream.Combine",
                ImmutableList.of(expectedEtlProducer1, expectedEtlProducer2, expectedEtlProducer3), 3);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void combinedMultiExtractorsCreatesJustOneExecutorProducer() throws Exception {
        EtlStream stream1 = EtlStream.extract(mockExtractor, mockExtractor2);
        EtlStream stream2 = EtlStream.extract(mockExtractor3);

        EtlStream.combine(stream1, stream2)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer1 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer2 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor2,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer3 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor3,
                expectedLoaderConsumer);

        EtlProducer expectedEtlProducer = etlProducerFactory.combineProducers("EtlStream.Combine",
                ImmutableList.of(expectedEtlProducer1, expectedEtlProducer2, expectedEtlProducer3), 3);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyCollectionOfExtractorsThrowsIllegalArgumentException() {
        EtlStream.extract(Collections.emptyList());
    }

    @Test
    public void singleExtractorInCollectionCreatesASingleProducer() throws Exception {
        EtlStream.extract(ImmutableList.of(mockExtractor))
                .transform(SimpleBeanClass.class, mockTransformer)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void canCombineTwoTerminatedEtlStreams() throws Exception {
        EtlStream stream1 = EtlStream.extract(mockExtractor)
                .transform(SimpleBeanClass.class, mockTransformer)
                .load(SimpleBeanClass.class, mockLoader);

        EtlStream stream2 = EtlStream.extract(mockExtractor2)
                .transform(SimpleBeanClass.class, mockTransformer2)
                .load(SimpleBeanClass.class, mockLoader2);

        EtlStream.combine(stream1, stream2)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer1 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);

        EtlConsumer expectedLoaderConsumer2 = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader2, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer2 = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer2, SimpleBeanClass.class, expectedLoaderConsumer2,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer2 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor2,
                expectedTransformerConsumer2);

        EtlProducer expectedEtlProducer = etlProducerFactory.combineProducers("EtlStream.Combine",
                ImmutableList.of(expectedEtlProducer1, expectedEtlProducer2), 2);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void canCombineTwoUnterminatedEtlStreams() throws Exception {
        EtlStream stream1 = EtlStream.extract(mockExtractor)
                .transform(SimpleBeanClass.class, mockTransformer);

        EtlStream stream2 = EtlStream.extract(mockExtractor2)
                .transform(SimpleBeanClass.class, mockTransformer2);

        EtlStream.combine(stream1, stream2)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer1 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);

        EtlConsumer expectedTransformerConsumer2 = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer2, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer2 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor2,
                expectedTransformerConsumer2);

        EtlProducer expectedEtlProducer = etlProducerFactory.combineProducers("EtlStream.Combine",
                ImmutableList.of(expectedEtlProducer1, expectedEtlProducer2), 2);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void canConstructASimpleEtlStreamWithMultipleExtractors() throws Exception {
        EtlStream.extract(mockExtractor, mockExtractor2, mockExtractor3)
                .transform(SimpleBeanClass.class, mockTransformer)
                .load(SimpleBeanClass.class, mockLoader)
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer1 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);
        EtlProducer expectedEtlProducer2 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor2,
                expectedTransformerConsumer);
        EtlProducer expectedEtlProducer3 = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor3,
                expectedTransformerConsumer);
        EtlProducer expectedEtlProducer = etlProducerFactory.combineProducers("EtlStream.Combine",
                ImmutableList.of(expectedEtlProducer1, expectedEtlProducer2, expectedEtlProducer3), 3);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void supportsCustomStageNames() throws Exception {
        EtlStream.from(extract(mockExtractor).withName("test-stage-1"))
                .then(transform(SimpleBeanClass.class, mockTransformer).withName("test-stage-2"))
                .then(load(SimpleBeanClass.class, mockLoader).withName("test-stage-3"))
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("test-stage-3", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("test-stage-3", getLogger("test-stage-3"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("test-stage-2",
                mockTransformer, SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("test-stage-2", getLogger("test-stage-2"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), defaultExecutor);

        EtlProducer expectedEtlProducer = etlProducerFactory.newExtractorProducer("test-stage-1", mockExtractor,
                expectedTransformerConsumer);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void supportsMultiWorkersWithDefaultQueueSize() throws Exception {
        EtlStream.extract(mockExtractor)
                .then(transform(SimpleBeanClass.class, mockTransformer).withThreads(2))
                .then(load(SimpleBeanClass.class, mockLoader).withThreads(3))
                .run(mockMetrics, mockEtlRunner);

        EtlConsumer expectedLoaderConsumer = etlConsumerFactory.newLoader("EtlStream.Load", mockLoader, SimpleBeanClass.class,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Load", getLogger("EtlStream.Load"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()), etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(3, 1000));

        EtlConsumer expectedTransformerConsumer = etlConsumerFactory.newTransformer("EtlStream.Transform", mockTransformer,
                SimpleBeanClass.class, expectedLoaderConsumer,
                etlConsumerFactory.newLogAsErrorConsumer("EtlStream.Transform", getLogger("EtlStream.Transform"),
                        SimpleBeanClass.class, new DefaultLoggingStrategy<>()),
                etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(2, 1000));

        EtlProducer expectedEtlProducer = etlProducerFactory.newExtractorProducer("EtlStream.Extract", mockExtractor,
                expectedTransformerConsumer);

        EtlProducer actualEtlProducer = getProducerFromRunner();

        assertThat(actualEtlProducer, equalTo(expectedEtlProducer));
    }

    @Test
    public void defaultRunnerInteractsWithEtlComponents() throws Exception {
        EtlStream.extract(mockExtractor)
                .transform(SimpleBeanClass.class, mockTransformer)
                .load(SimpleBeanClass.class, mockLoader)
                .run();

        verify(mockExtractor, times(1)).open(null);
        verify(mockExtractor, times(2)).next();
        verify(mockExtractor, times(1)).close();
        verify(mockTransformer, times(1)).open(null);
        verify(mockTransformer, times(1)).transform(eq(simpleObject));
        verify(mockTransformer, times(1)).close();
        verify(mockLoader, times(1)).open(null);
        verify(mockLoader, times(1)).load(eq(simpleObject));
        verify(mockLoader, times(1)).close();
    }

    @Test(expected= IllegalStateException.class)
    public void transformerAfterLoaderThrowsIllegalStateException() {
        EtlStream.extract(mockExtractor)
                .load(SimpleBeanClass.class, mockLoader)
                .transform(SimpleBeanClass.class, mockTransformer);
    }

    @Test(expected= IllegalStateException.class)
    public void loaderAfterLoaderThrowsIllegalStateException() {
        EtlStream.extract(mockExtractor)
                .load(SimpleBeanClass.class, mockLoader)
                .load(SimpleBeanClass.class, mockLoader);
    }

    @Test(expected= IllegalStateException.class)
    public void loaderAfterCombinedTerminalStreamsThrowsIllegalStateException() {
        EtlStream.combine(EtlStream.extract(mockExtractor).load(SimpleBeanClass.class, mockLoader),
                EtlStream.extract(mockExtractor).load(SimpleBeanClass.class, mockLoader))
                .load(SimpleBeanClass.class, mockLoader);
    }

    @Test
    public void loaderAfterCombinedStreamsWhereOnlyOneIsTerminatedDoesNotThrow() {
        EtlStream.combine(EtlStream.extract(mockExtractor).load(SimpleBeanClass.class, mockLoader),
                EtlStream.extract(mockExtractor))
                .load(SimpleBeanClass.class, mockLoader);
    }
}
