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

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.amazon.pocketEtl.core.producer.EtlProducerFactory;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

@Getter
class EtlStageChain {
    private final static String DEFAULT_COMBINE_STAGE_NAME = "EtlStream.Combine";

    private final EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();
    private final EtlProducerFactory etlProducerFactory = new EtlProducerFactory(etlExecutorFactory);
    private final ImmutableList<EtlConsumerStage> consumerStagesStack;
    private final EtlProducerStage headStage;

    EtlStageChain(EtlStageChain priorChain, EtlConsumerStage newConsumerStage) {
        consumerStagesStack = ImmutableList.<EtlConsumerStage>builder()
                .add(newConsumerStage)
                .addAll(priorChain.getConsumerStagesStack())
                .build();

        this.headStage = priorChain.getHeadStage();
    }

    EtlStageChain(EtlProducerStage headStage) {
        consumerStagesStack = ImmutableList.of();
        this.headStage = headStage;
    }

    @Nullable
    private EtlConsumer constructConsumerChain(@Nullable EtlConsumer downstreamConsumer) {
        // Although forEachRemaining in this context is guaranteed to execute in series, java 8 will not allow
        // overwriting a non-final variable from within a closure so a final AtomicReference is used to wrap the dynamic
        // value.
        AtomicReference<EtlConsumer> consumerChainHead = new AtomicReference<>(downstreamConsumer);

        getConsumerStagesStack().iterator().forEachRemaining(stage ->
                consumerChainHead.set(stage.constructConsumerForStage(consumerChainHead.get())));

        return consumerChainHead.get();
    }

    EtlProducer constructProducer() {
        Collection<EtlProducer> etlProducers = constructComponentProducers(null);
        return etlProducers.size() == 1 ? etlProducers.iterator().next() :
                etlProducerFactory.combineProducers(DEFAULT_COMBINE_STAGE_NAME, etlProducers, etlProducers.size());
    }

    Collection<EtlProducer> constructComponentProducers(@Nullable EtlConsumer downstreamConsumer) {
        return getHeadStage().constructProducersForStage(constructConsumerChain(downstreamConsumer));
    }
}
