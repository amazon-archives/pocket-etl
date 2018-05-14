/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.EtlMetrics;

import javax.annotation.Nullable;
import java.util.prefs.BackingStoreException;

/**
 * Producer interface for ETL system. A producer will, once started, continue to produce work until the supply of
 * work has been exhausted. Typically a producer will send work it produces to a downstream consumer of some kind.
 */
public interface EtlProducer extends AutoCloseable {
    /**
     * Produce objects until the source of objects has been exhausted.
     *
     * @throws BackingStoreException If the backing store this producer is producing work from has a problem.
     * @throws IllegalStateException If the producer is not in a state capable of producing new work.
     */
    void produce() throws BackingStoreException, IllegalStateException;

    /**
     * Signal the producer to complete its work and free any resources allocated for the production of work.
     *
     * @throws Exception If something goes wrong.
     */
    @Override
    void close() throws Exception;

    /**
     * Signal the producer that it should prepare to produce work.
     * @param parentMetrics An EtlMetrics object to add counters and timers to, will be null if profiling is not
     *                      required.
     */
    void open(@Nullable EtlMetrics parentMetrics);
}
