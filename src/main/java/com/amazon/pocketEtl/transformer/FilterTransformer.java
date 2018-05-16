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

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.lookup.Lookup;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * An implementation of transformer that acts as a filter. This type of transformer does not modify the object itself
 * therefore only has one generic type that is applied as both the upstream and downstream types.
 * @param <T> The type of object being filtered.
 */
public class FilterTransformer<T> implements Transformer<T, T> {
    private final BiPredicate<T, Lookup<T, T>> filter;
    private final Lookup<T, T> filterSetLookup;

    /**
     * Standard constructor.
     * @param filter A filter used to evaluate whether the object should be filtered.
     * @param filterSetLookup A lookup object to be used by the filter to compare against.
     */
    public FilterTransformer(BiPredicate<T, Lookup<T, T>> filter, Lookup<T, T> filterSetLookup) {
        this.filter = filter;
        this.filterSetLookup = filterSetLookup;
    }

    /**
     * Filters a single object.
     * @param objectToTransform The object to be filtered.
     * @return Will return an empty list if the object does not meet the filter criteria, or a list with a single
     * instance of the object if it does.
     */
    @Override
    public List<T> transform(T objectToTransform) {
        return filter.test(objectToTransform, filterSetLookup) ? ImmutableList.of(objectToTransform) : ImmutableList.of();
    }

    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        //no-op
    }

    @Override
    public void close() throws Exception {
        //no-op
    }
}
