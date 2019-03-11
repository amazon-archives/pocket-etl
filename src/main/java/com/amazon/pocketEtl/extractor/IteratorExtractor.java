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

package com.amazon.pocketEtl.extractor;

import java.util.Iterator;
import java.util.Optional;

import com.amazon.pocketEtl.Extractor;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Simple static builder that constructs an extractor around a simple Java Iterator object.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class IteratorExtractor<T> implements Extractor<T> {
    private final Iterator<T> iterator;

    /**
     * Simple static builder that constructs an extractor around a simple Java Iterator object.
     * @param iterator A standard iterator.
     * @param <T> Type of object the iterator iterates on.
     * @return An Extractor object that can be used in Pocket ETL jobs.
     */
    public static <T> Extractor<T> of(Iterator<T> iterator) {
        return new IteratorExtractor<>(iterator);
    }

    @Override
    public Optional<T> next() {
        return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    }
}
