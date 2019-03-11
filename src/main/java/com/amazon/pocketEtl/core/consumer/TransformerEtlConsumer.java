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
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Consumer implementation that wraps a Transformer object. This will effectively map and transform an object of one
 * type to an object of another type and then pass that object along to a downstream consumer. If something goes wrong
 * during the transformation the untransformed object is instead routed to a consumer designated for handling errors.
 *
 * @param <UpstreamType>   Object type being passed into the transformer.
 * @param <DownstreamType> Object type being produced after the transformation.
 */
@EqualsAndHashCode
class TransformerEtlConsumer<UpstreamType, DownstreamType> implements EtlConsumer {
    private final static Logger logger = getLogger(TransformerEtlConsumer.class);

    private final String name;
    private final EtlConsumer downstreamEtlConsumer;
    private final EtlConsumer errorEtlConsumer;
    private final Transformer<UpstreamType, DownstreamType> transformer;
    private final Class<UpstreamType> transformerUpstreamTypeClass;
    private EtlMetrics parentMetrics = null;

    /**
     * Standard constructor.
     *
     * @param name                          A human readable name for the instance of this class that will be used in
     *                                      logging and metrics.
     * @param downstreamEtlConsumer            Consumer object to pass transformed objects into.
     * @param errorEtlConsumer                 Consumer object to pass objects into that could not be transformed.
     * @param transformer                   Transformer object to perform the transformations.
     * @param transformerUpstreamTypeClass  Class for objects being consumed by the transformer (upstream type).
     */
    TransformerEtlConsumer(String name,
                           EtlConsumer downstreamEtlConsumer,
                           EtlConsumer errorEtlConsumer,
                           Transformer<UpstreamType, DownstreamType> transformer,
                           Class<UpstreamType> transformerUpstreamTypeClass) {
        this.name = name;
        this.downstreamEtlConsumer = downstreamEtlConsumer;
        this.errorEtlConsumer = errorEtlConsumer;
        this.transformer = transformer;
        this.transformerUpstreamTypeClass = transformerUpstreamTypeClass;
    }

    /**
     * Consume and transform a single object then pass the transformed object to a downstream consumer. If the object
     * cannot be transformed it will instead by passed as-is to the designated error consumer this object was
     * constructed with.
     *
     * @param objectToTransform The object to be transformed.
     * @throws IllegalStateException If the consumer is in a state that cannot accept work.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     *                                             detected and the stream needs to be aborted.
     */
    @Override
    public void consume(EtlStreamObject objectToTransform) throws IllegalStateException,
                                                                  UnrecoverableStreamFailureException {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "TransformerConsumer." + name +
                                                                              ".consume")) {

            List<DownstreamType> transformedObjects;

            try {
                transformedObjects = transformer.transform(objectToTransform.get(transformerUpstreamTypeClass));
            } catch (UnrecoverableStreamFailureException e) {
                logger.error("Unrecoverable stream exception thrown in transformer object, aborting stream: ", e);
                throw e;
            } catch (RuntimeException e) {
                logger.warn("Exception thrown in transformer object: ", e);
                errorEtlConsumer.consume(objectToTransform);
                return;
            }

            // Optimized path for single object transformation, re-uses existing EtlStreamObject
            if (transformedObjects.size() == 1) {
                downstreamEtlConsumer.consume(objectToTransform.with(transformedObjects.get(0)));
            } else {
                transformedObjects.forEach(
                    obj -> downstreamEtlConsumer.consume(objectToTransform.createCopy().with(obj)));
            }
        }
    }

    /**
     * Signals the consumer to prepare to accept work. The designated downstream consumers will also be signalled.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "TransformerConsumer." + name + ".open")) {
            transformer.open(parentMetrics);
            downstreamEtlConsumer.open(parentMetrics);
            errorEtlConsumer.open(parentMetrics);
        }
    }

    /**
     * Signals the consumer to stop accepting work and complete any buffered work. The designated downstream consumers
     * will also be signaled.
     *
     * @throws Exception if something went wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "TransformerConsumer." + name + ".close")) {
            try {
                downstreamEtlConsumer.close();
            } catch (UnrecoverableStreamFailureException e) {
                throw e;
            } catch (RuntimeException e) {
                logger.warn("Exception thrown closing downstream EtlConsumer object: ", e);
            }
            try {
                transformer.close();
            } catch (UnrecoverableStreamFailureException e) {
                throw e;
            } catch (RuntimeException e) {
                logger.warn("Exception thrown closing transformer object: ", e);
            }

            errorEtlConsumer.close();
        }
    }
}