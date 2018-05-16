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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.core.EtlStreamObject;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Implementation of consumer that wraps another consumer and keeps track of how many upstream producers this
 * consumer has and will prevent the downstream consumer from being closed until all of the upstream producers have
 * called close(). The class derives it's name from the concept of 'smart pointers' which keep a count of references.
 * This wrapper is required for any singleton-type consumer that is linked to multiple upstream producers and should
 * wrap all the other consumer implementations for a given ETL stage.
 * <p>
 * Currently cyclic connections in the consumer graph are not supported and will make it impossible to close the
 * consumer, although it's noted here as possible future expansion for this class.
 */
@EqualsAndHashCode(exclude = {"openCount"})
class SmartEtlConsumer implements EtlConsumer {
    private final static Logger logger = getLogger(SmartEtlConsumer.class);

    private final String name;

    @Getter(AccessLevel.PACKAGE)
    private final EtlConsumer wrappedEtlConsumer;

    private final AtomicInteger openCount = new AtomicInteger(0);

    private EtlMetrics parentMetrics = null;

    /**
     * Standard constructor.
     *
     * @param name            A human readable name for the instance of this class that will be used in logging and metrics.
     * @param wrappedEtlConsumer The downstream consumer wrapped by this consumer.
     */
    SmartEtlConsumer(String name, EtlConsumer wrappedEtlConsumer) {
        this.name = name;
        this.wrappedEtlConsumer = wrappedEtlConsumer;
    }

    /**
     * Passes an object to be consumed directly to the wrapped consumer.
     *
     * @param objectToConsume The object to be consumed.
     */
    @Override
    public void consume(EtlStreamObject objectToConsume) {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SmartConsumer." + name + ".consume")) {
            wrappedEtlConsumer.consume(objectToConsume);
        }
    }

    /**
     * Signals the consumer to prepare to accept work. This implementation will keep a count of how many times it
     * has been opened as a way of knowing how many upstream producers expect to use it. It will also pass on this
     * signal to the wrapped consumer the first time, and only the first time it is called.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SmartConsumer." + name + ".open")) {
            if (openCount.incrementAndGet() == 1) {
                wrappedEtlConsumer.open(parentMetrics);
            }
        }
    }

    /**
     * Signals the consumer that the batch has finished and it should empty and finalize any buffers. For this
     * implementation nothing will happen until all of the upstream producers have called close at which point it
     * will signal the wrapped consumer to close.
     *
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SmartConsumer." + name + ".close")) {
            int newOpenCount = openCount.decrementAndGet();

            if (newOpenCount == 0) {
                wrappedEtlConsumer.close();
            } else if (newOpenCount < 0) {
                IllegalStateException e = new IllegalStateException("Attempt to close consumer that is not open");
                logger.error("Exception thrown closing consumer: ", e);
                throw e;
            }
        }
    }
}
