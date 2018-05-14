/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.core.EtlStreamObject;

import javax.annotation.Nullable;

/**
 * Common interface for all ETL objects that consume data from the ETL stream.
 */
public interface EtlConsumer extends AutoCloseable {
    /**
     * Consume a single object.
     *
     * @param objectToConsume The object to be consumed.
     * @throws IllegalStateException If the consumer is in a state that it cannot accept more objects to consume.
     */
    void consume(EtlStreamObject objectToConsume) throws IllegalStateException;

    /**
     * Signal the consumer to prepare receiving work.
     *
     * @param parentMetrics An EtlMetrics object to store counters and timers. Will be null if profiling is not
     *                      required.
     */
    void open(@Nullable EtlMetrics parentMetrics);
}
