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

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.exception.GenericEtlException;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Producer implementation that facilitates running parallel producers. This is typically the top-most object in a
 * complex chain of Producers and Consumers that constitutes an ETL job. The job would be considered complete once all
 * the producers have exhausted their supply of work and been closed.
 */
@EqualsAndHashCode(exclude = "abortStreamException")
class ExecutorEtlProducer implements EtlProducer {
    private final static Logger logger = getLogger(ExecutorEtlProducer.class);

    private final String name;
    private final Collection<EtlProducer> etlProducers;
    private final EtlExecutor etlExecutor;

    private AtomicReference<UnrecoverableStreamFailureException> abortStreamException = new AtomicReference<>();
    private EtlMetrics parentMetrics = null;

    /**
     * Standard constructor.
     *
     * @param etlProducers   List of producers to be run in parallel.
     * @param etlExecutor An EtlExecutor object to manage the thread-pool to run the producers in.
     */
    ExecutorEtlProducer(String name, Collection<EtlProducer> etlProducers, EtlExecutor etlExecutor) {
        this.name = name;
        this.etlProducers = etlProducers;
        this.etlExecutor = etlExecutor;
    }

    /**
     * Begin running all the wrapped producers in parallel. Each producer will be closed by its executing thread once it
     * has completed producing all the work it is able to. This call will not necessarily block on completion of that
     * work, only close() will do that.
     *
     * @throws IllegalStateException if the producer is in a state that is unable to produce new work.
     */
    @Override
    public void produce() throws IllegalStateException {
        logger.info("Running " + etlProducers.size() + " producers.");

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorProducer." + name + ".produce")) {
            etlProducers.forEach(producer -> etlExecutor.submit(() -> {
                try {
                    producer.produce();
                } catch (UnrecoverableStreamFailureException e) {
                    abortStreamException.set(e);
                } catch (RuntimeException e) {
                    logger.error("Error extracting data: ", e);
                }

                try {
                    producer.close();
                } catch (UnrecoverableStreamFailureException e) {
                    abortStreamException.set(e);
                } catch (Exception e) {
                    logger.error("Error closing producer: ", e);
                }
            }, parentMetrics));
        }
    }

    /**
     * Block and wait for each wrapped Producer to complete its production of work and be closed, and then shutdown
     * the EtlExecutor running them.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorProducer." + name + ".close")) {
            try {
                etlExecutor.shutdown();
            } catch (GenericEtlException e) {
                logger.error("Error shutting down executor: ", e);
            }

            if (abortStreamException.get() != null) {
                throw abortStreamException.get();
            }
        }
    }

    /**
     * Signal the producer that it should prepare to produce work. This in turn will signal all the wrapped producers
     * the same.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExecutorProducer." + name + ".open")) {
            etlProducers.forEach(producer -> producer.open(parentMetrics));
        }
    }
}
