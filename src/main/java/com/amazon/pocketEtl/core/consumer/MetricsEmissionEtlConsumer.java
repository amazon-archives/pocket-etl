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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.core.EtlStreamObject;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Implementation of consumer that logs a counter for each record consumed in the format :
 * job-name.stage-name.recordsProcessed
 * This consumer is automatically inserted into the standard chain of consumers that wrap ETL consumers.
 */
@EqualsAndHashCode
class MetricsEmissionEtlConsumer implements EtlConsumer {
    private final String stageName;

    @Getter(AccessLevel.PACKAGE)
    private final EtlConsumer downstreamEtlConsumer;

    private EtlMetrics parentMetrics;

    MetricsEmissionEtlConsumer(String stageName, EtlConsumer downstreamEtlConsumer) {
      this.stageName = stageName;
      this.downstreamEtlConsumer = downstreamEtlConsumer;
    }

    @Override
    public void consume(EtlStreamObject objectToConsume) throws IllegalStateException {
        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "MetricsEmissionConsumer." + stageName + ".consume")) {
            scope.addCounter(stageName + ".recordsProcessed", 1);
            downstreamEtlConsumer.consume(objectToConsume);
        }
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "MetricsEmissionConsumer." +
                                                                            stageName +
                                                                            ".open")) {
            scope.addCounter(stageName + ".recordsProcessed", 0);
            this.parentMetrics = parentMetrics;
            downstreamEtlConsumer.open(parentMetrics);
        }
    }

    @Override
    public void close() throws Exception {
        downstreamEtlConsumer.close();
    }
}
