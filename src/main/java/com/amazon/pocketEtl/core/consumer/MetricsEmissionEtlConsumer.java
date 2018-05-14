/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
            scope.addCounter(stageName + ".recordsProcessed", 1) ;
            downstreamEtlConsumer.consume(objectToConsume);
        }
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
        downstreamEtlConsumer.open(parentMetrics);
    }

    @Override
    public void close() throws Exception {
        downstreamEtlConsumer.close();
    }
}
