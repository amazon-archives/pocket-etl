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
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.Logger;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Implementation of Consumer that wraps a Loader object and passes all objects to be consumed into the loader. If
 * something goes wrong during the loading the object is instead routed to a consumer designated for handling errors.
 *
 * @param <UpstreamType> Type of object to be consumed/loaded.
 */
@EqualsAndHashCode
class LoaderEtlConsumer<UpstreamType> implements EtlConsumer {
    private final static Logger logger = getLogger(LoaderEtlConsumer.class);

    private final String name;
    private final Loader<UpstreamType> loader;
    private final EtlConsumer errorEtlConsumer;
    private final Class<UpstreamType> loaderTypeClass;

    private EtlMetrics parentMetrics;

    /**
     * Standard constructor.
     *
     * @param name             A human readable name for the instance of this class that will be used in logging and metrics.
     * @param loader           Wrapped loader object.
     * @param loaderTypeClass  Class definition for the objects being loaded by the wrapped loader.
     * @param errorEtlConsumer    Consumer to send objects to that could not be loaded.
     */
    LoaderEtlConsumer(String name, Loader<UpstreamType> loader, Class<UpstreamType> loaderTypeClass, EtlConsumer errorEtlConsumer) {
        this.name = name;
        this.loader = loader;
        this.loaderTypeClass = loaderTypeClass;
        this.errorEtlConsumer = errorEtlConsumer;
    }

    /**
     * Consumes a single object to be loaded by the loader.
     *
     * @param objectToLoad The object to be loaded.
     * @throws IllegalStateException If the consumer is in a state that cannot accept more objects to be loaded.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     *                                             detected and the stream needs to be aborted.
     */
    @Override
    public void consume(EtlStreamObject objectToLoad) throws IllegalStateException, UnrecoverableStreamFailureException {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "LoaderConsumer." + name + ".consume")) {
            try {
                loader.load(objectToLoad.get(loaderTypeClass));
            } catch (UnrecoverableStreamFailureException e) {
                logger.error("Unrecoverable stream exception thrown in loader object, aborting stream: ", e);
                throw e;
            } catch (RuntimeException e) {
                logger.warn("Exception thrown in loader object: ", e);
                errorEtlConsumer.consume(objectToLoad);
            }
        }
    }

    /**
     * Signals the loader to prepare to accept work. This will also signal the error consumer attached to this object.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "LoaderConsumer." + name + ".open")) {
            loader.open(parentMetrics);
            errorEtlConsumer.open(parentMetrics);
        }
    }

    /**
     * Signals the loader that the batch is complete and any buffers should be flushed and finalized. This will also
     * close the error consumer attached to this object.
     *
     * @throws Exception If something went wrong closing the loader.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "LoaderConsumer." + name + ".close")) {
            try {
                loader.close();
            } catch (UnrecoverableStreamFailureException e) {
                throw e;
            } catch (RuntimeException e) {
                logger.warn("Exception thrown closing loader object: ", e);
            }

            errorEtlConsumer.close();
        }
    }
}