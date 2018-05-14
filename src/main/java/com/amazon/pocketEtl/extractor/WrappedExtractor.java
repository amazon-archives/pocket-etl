/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Extractor;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.prefs.BackingStoreException;

import static lombok.AccessLevel.PROTECTED;

/**
 * Allows a class to masquerade as an Extractor by extending this class and providing an implementation of a method
 * to get the real Extractor it is masquerading as. Used by classes that build complex Extractor objects but don't have
 * any real implementation themselves such as S3BufferedExtractor.
 * @param <T> The type of object being extracted.
 */
@RequiredArgsConstructor(access = PROTECTED)
public abstract class WrappedExtractor<T> implements Extractor<T> {
    protected abstract Extractor<T> getWrappedExtractor();

    @Override
    public Optional<T> next() throws BackingStoreException {
        return getWrappedExtractor().next();
    }

    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        getWrappedExtractor().open(parentMetrics);
    }

    @Override
    public void close() throws Exception {
        getWrappedExtractor().close();
    }
}
