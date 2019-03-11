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

package com.amazon.pocketEtl;

import javax.annotation.Nullable;
import java.util.List;

import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

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
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     * detected and the stream needs to be aborted.
     */
    List<DownstreamType> transform(UpstreamType objectToTransform) throws UnrecoverableStreamFailureException;

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
