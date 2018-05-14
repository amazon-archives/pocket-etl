/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.Extractor;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;
import java.util.Optional;

/**
 * Simple static builder that constructs an extractor around a simple Java Iterator object.
 */
@SuppressWarnings("WeakerAccess")
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
