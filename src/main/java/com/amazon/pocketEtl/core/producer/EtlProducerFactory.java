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

import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import lombok.RequiredArgsConstructor;

import java.util.Collection;

/**
 * An injectable factory class for building various types of useful Producer implementations. This is the only
 * way you should be constructing Producer objects of any kind outside of this package. Producers of various kinds
 * should be combined into a single Producer using combineProducers() which will then run all the producers it is
 * constructed with together.
 */
@RequiredArgsConstructor
public class EtlProducerFactory {
    private final EtlExecutorFactory etlExecutorFactory;

    /**
     * Constructs a new producer based on an Extractor.
     * @param name The name of this producer used in logging and reporting.
     * @param extractor The extractor object this producer is based on.
     * @param downstreamEtlConsumer The consumer to send objects this producer extracts to.
     * @param <T> The type of object being extracted.
     * @return A fully constructed producer.
     */
    public <T> EtlProducer newExtractorProducer(String name, Extractor<T> extractor, EtlConsumer downstreamEtlConsumer) {
        return new ExtractorEtlProducer<>(name, downstreamEtlConsumer, extractor);
    }

    /**
     * Combines multiple producers into a single producer object that behaves like a single producer but drives all
     * the producers it was constructed with.
     * @param name The name of this producer used in logging and reporting.
     * @param etlProducers A collection of producers to combine into a single producer.
     * @param numberOfParallelWorkers The degree of parallelism to drive the producers with.
     * @return A fully constructed producer.
     */
    public EtlProducer combineProducers(String name, Collection<EtlProducer> etlProducers, int numberOfParallelWorkers) {
        return new ExecutorEtlProducer(name, etlProducers, etlExecutorFactory.newUnboundFixedThreadsEtlExecutorFactory(numberOfParallelWorkers));
    }
}
