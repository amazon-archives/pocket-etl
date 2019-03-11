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

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Nonnull;

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.producer.EtlProducer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Abstract class that represents the producer stage in an EtlStream. An EtlStream consists of a producer stage chained
 * to one or more consumer stages.
 */
@Getter(AccessLevel.PACKAGE)
@RequiredArgsConstructor
public abstract class EtlProducerStage {
    /**
     * Static constructor for an EtlProducerStage that extracts data and puts it on the stream. Used as a component in an
     * EtlStream.
     * @param extractor An extractor that extracts data from somewhere to be put on the stream.
     * @return An EtlProducerStage that can be used as a component for an EtlStream.
     */
    public static EtlProducerStage extract(@Nonnull Extractor<?> extractor) {
        return EtlExtractStage.of(extractor);
    }

    /**
     * Static constructor for an EtlProducerStage that extracts data from multiple sources and puts it on the stream.
     * Used as a component in an EtlStream. The extractors will run in parallel when the stream is run.
     * @param extractors A collection of extractors that extract data from somewhere to be put on the stream.
     * @return An EtlProducerStage that can be used as a component for an EtlStream.
     */
    public static EtlProducerStage extract(@Nonnull Extractor<?> ...extractors) {
        return EtlExtractStage.of(Arrays.asList(extractors));
    }

    /**
     * Static constructor for an EtlProducerStage that extracts data from multiple sources and puts it on the stream.
     * Used as a component in an EtlStream. The extractors will run in parallel when the stream is run.
     * @param extractors A collection of extractors that extract data from somewhere to be put on the stream.
     * @return An EtlProducerStage that can be used as a component for an EtlStream.
     */
    public static EtlProducerStage extract(@Nonnull Collection<Extractor<?>> extractors) {
        return EtlExtractStage.of(extractors);
    }

    /**
     * Static constructor for an EtlProducerStage that is composed from other EtlStream objects. The combined streams
     * will run as a black-boxed producer in its own right in the stream and if any of those streams are not terminated,
     * data will flow out of the stage into other consumer stages attached to this one.
     * @param streamsToCombine A collection of EtlStream objects to combine into a single stage of another stream.
     * @return An EtlProducerStage that can be used as a component for an EtlStream.
     */
    public static EtlProducerStage combine(@Nonnull Collection<EtlStream> streamsToCombine) {
        return EtlCombineStage.of(streamsToCombine);
    }

    /**
     * Construct a new EtlProducerStage object that is the copy of an existing one but with a new specific value.
     * @param stageName This is a human readable name for the stage that is used in profiling and logging and will help you
     *             build specific monitors and alarms for specific jobs or stages of those jobs.
     * @return A new EtlProducerStage object.
     */
    public abstract EtlProducerStage withName(@Nonnull String stageName);

    /***************************************************************************************************************/

    private final String stageName;

    abstract Collection<EtlProducer> constructProducersForStage(EtlConsumer downstreamConsumer);
}
