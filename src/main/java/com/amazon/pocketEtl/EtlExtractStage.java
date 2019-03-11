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

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.amazon.pocketEtl.core.producer.EtlProducerFactory;

import lombok.AccessLevel;
import lombok.Getter;

@Getter(AccessLevel.PACKAGE)
class EtlExtractStage extends EtlProducerStage {
    private final static String DEFAULT_EXTRACT_STAGE_NAME = "EtlStream.Extract";
    private final static EtlExecutorFactory defaultExecutorFactory = new EtlExecutorFactory();
    private final static EtlProducerFactory defaultProducerFactory = new EtlProducerFactory(defaultExecutorFactory);

    private final Collection<Extractor<?>> extractors;
    private final EtlProducerFactory etlProducerFactory;

    EtlExtractStage(@Nonnull Collection<Extractor<?>> extractors,
                            @Nonnull String stageName,
                            @Nonnull EtlProducerFactory etlProducerFactory) {
        super(stageName);

        if (extractors.isEmpty()) {
            throw new IllegalArgumentException("An EtlStream must be constructed with at least one extractor.");
        }

        this.extractors = extractors;
        this.etlProducerFactory = etlProducerFactory;
    }

    @Override
    public EtlExtractStage withName(@Nonnull String stageName) {
        return new EtlExtractStage(getExtractors(), stageName, getEtlProducerFactory());
    }

    static EtlExtractStage of(@Nonnull Extractor<?> extractor) {
        return new EtlExtractStage(Collections.singletonList(extractor), DEFAULT_EXTRACT_STAGE_NAME, defaultProducerFactory);
    }

    static EtlExtractStage of(@Nonnull Collection<Extractor<?>> extractors) {
        return new EtlExtractStage(extractors, DEFAULT_EXTRACT_STAGE_NAME, defaultProducerFactory);
    }

    @Override
    Collection<EtlProducer> constructProducersForStage(EtlConsumer downstreamConsumer) {
        return extractors.stream().map(extractor ->
                getEtlProducerFactory().newExtractorProducer(getStageName(), extractor, downstreamConsumer))
                .collect(Collectors.toList());
    }
}
