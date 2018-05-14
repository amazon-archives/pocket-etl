/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for an object that transforms an object of one type into a stream of objects of another type. This allows
 * for an expansion or contraction of the stream where one object can become many objects or no objects.
 *
 * @param <UpstreamType>   The object type before transformation.
 * @param <DownstreamType> The object type after transformation.
 */
@FunctionalInterface
public interface Transformer<UpstreamType, DownstreamType> extends AutoCloseable {
    /**
     * Transform a single object.
     *
     * @param objectToTransform The object to be transformed.
     * @return The transformed object.
     */
    List<DownstreamType> transform(UpstreamType objectToTransform);

    /**
     * Signal the transformer to prepare to transform objects.
     *
     * @param parentMetrics An EtlMetrics object to attach any counters or timers to, will be null if profiling is not
     *                      required.
     */
    default void open(@Nullable EtlMetrics parentMetrics) {
        //no-op
    }

    /**
     * Free up any resources allocated for the purposes of transforming objects.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    default void close() throws Exception {
        //no-op
    }
}
