/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Collectors;

@Getter(AccessLevel.PACKAGE)
@EqualsAndHashCode(callSuper = true)
class EtlCombineStage extends EtlProducerStage {
    private final static String DEFAULT_COMBINE_STAGE_NAME = "EtlStream.Combine";

    private final Collection<EtlStageChain> stageChains;

    private EtlCombineStage(@Nonnull Collection<EtlStageChain> stageChains, @Nonnull String stageName) {
        super(stageName);

        if (stageChains.size() < 2) {
            throw new IllegalArgumentException("A combined EtlStream must be constructed with at least two component streams.");
        }

        this.stageChains = stageChains;
    }

    static EtlCombineStage of(@Nonnull Collection<EtlStream> streamsToCombine) {
        Collection<EtlStageChain> stageChains = streamsToCombine.stream().map(EtlStream::getStageChain).collect(Collectors.toList());
        return new EtlCombineStage(stageChains, DEFAULT_COMBINE_STAGE_NAME);
    }

    @Override
    public EtlCombineStage withName(@Nonnull String stageName) {
        return new EtlCombineStage(getStageChains(), stageName);
    }

    @Override
    Collection<EtlProducer> constructProducersForStage(EtlConsumer downstreamConsumer) {
        return getStageChains().stream()
                .map(stageChain -> stageChain.constructComponentProducers(downstreamConsumer))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        }
}
