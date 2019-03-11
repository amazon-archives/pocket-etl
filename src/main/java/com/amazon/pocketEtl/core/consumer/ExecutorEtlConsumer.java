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
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.logging.log4j.Logger;

import static org.apache.logging.log4j.LogManager.getLogger;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumer implementation that wraps another consumer and facilitates parallel consumption. The wrapped consumer's
 * consume() method must be threadsafe for this to work. Note that open() and close() are not executed in parallel
 * threads.
 */
@EqualsAndHashCode(exclude = "abortStreamException")
class ExecutorEtlConsumer implements EtlConsumer {
    private final static Logger logger = getLogger(ExecutorEtlConsumer.class);

    private final String name;

    @Getter(AccessLevel.PACKAGE)
    private final EtlConsumer wrappedEtlConsumer;

    private final EtlExecutor etlExecutor;
    private AtomicReference<UnrecoverableStreamFailureException> abortStreamException = new AtomicReference<>();
    private EtlMetrics parentMetrics = null;

    /**
     * Standard constructor.
     *
     * @param name            A human readable name for the instance of this class that will be used in logging and metrics.
     * @param wrappedEtlConsumer Wrapped consumer object. The consume() method of this consumer must be threadsafe.
     * @param etlExecutor     An EtlExecutor object to facilitate the parallel consumption.
     */
    ExecutorEtlConsumer(String name, EtlConsumer wrappedEtlConsumer, EtlExecutor etlExecutor) {
        this.name = name;
        this.wrappedEtlConsumer = wrappedEtlConsumer;
        this.etlExecutor = etlExecutor;
    }

    /**
     * Blocks and drains any remaining work left to do by the threads managed in this object. It will then signal
     * the wrapped consumer to close.
     *
     * @throws Exception If something went wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorConsumer." + name + ".close")) {
            etlExecutor.shutdown();
            wrappedEtlConsumer.close();
        }

        checkForAbortedStream();
    }

    /**
     * Asynchronously accepts an object to be consumed by the wrapped consumer. The request will be queued and worked
     * on by the pool of threads managed by this object. It is therefore important that the wrapped consumer's consume()
     * method is threadsafe.
     *
     * @param objectToConsume The object to be consumed.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     *                                             detected and the stream needs to be aborted.
     */
    @Override
    public void consume(EtlStreamObject objectToConsume) throws UnrecoverableStreamFailureException {
        checkForAbortedStream();

        if (etlExecutor.isShutdown()) {
            IllegalStateException e = new IllegalStateException("Transformer was closed and cannot receive more loader requests");
            logger.error("Error inside multi-threaded transformation: ", e);
            throw e;
        }

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorConsumer." + name + ".consume")) {
            etlExecutor.submit(() -> {
                    if (abortStreamException.get() != null) {
                        return;
                    }

                    try {
                        wrappedEtlConsumer.consume(objectToConsume);
                    } catch (UnrecoverableStreamFailureException e) {
                        abortStreamException.set(e);
                    }
                }, parentMetrics);
        }
    }

    /**
     * Signals the consumer that it should prepare to receive work. This in turn will call open() on the wrapped
     * consumer.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorConsumer." + name + ".open")) {
            this.parentMetrics = parentMetrics;
            wrappedEtlConsumer.open(parentMetrics);
        }
    }

    private void checkForAbortedStream() {
        if (abortStreamException.get() != null) {
            throw abortStreamException.get();
        }
    }
}
