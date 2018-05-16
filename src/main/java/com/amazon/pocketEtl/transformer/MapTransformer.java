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

package com.amazon.pocketEtl.transformer;

import com.amazon.pocketEtl.Transformer;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.function.Function;

/**
 * Simple static constructor for the most basic type of Transformer that maps a single object to another single object,
 * effectively a map() operation.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class MapTransformer<T, R> implements Transformer<T, R> {
    private final Function<T, R> mapFunction;

    /**
     * Construct a new transformer that will map a single object to another single object.
     * @param mapFunction Lambda function to perform the transformation.
     * @param <T> Type of object being mapped.
     * @param <R> Type of object being mapped to.
     * @return A new Transformer object that can be used in Pocket ETL jobs.
     */
    public static <T, R> MapTransformer<T, R> of(Function<T, R> mapFunction) {
        return new MapTransformer<>(mapFunction);
    }

    @Override
    public List<R> transform(T objectToTransform) {
        return ImmutableList.of(mapFunction.apply(objectToTransform));
    }
}
