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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.producer.EtlProducer;

import lombok.AccessLevel;
import lombok.Getter;

@Getter(AccessLevel.PACKAGE)
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
