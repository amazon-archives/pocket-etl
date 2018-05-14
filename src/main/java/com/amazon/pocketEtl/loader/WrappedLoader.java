/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Loader;

import javax.annotation.Nullable;

/**
 * Allows a class to masquerade as a Loader by extending this class and providing an implementation of a method to get
 * the real Loader it is masquerading as. Used by classes that build complex Loader objects but don't have any real
 * implementation themselves such as RedshiftBulkLoader.
 * @param <T> The type of object being loaded.
 */
public abstract class WrappedLoader<T> implements Loader<T> {
    protected abstract Loader<T> getWrappedLoader();

    @Override
    public void load(T objectToLoad) {
        getWrappedLoader().load(objectToLoad);
    }

    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        getWrappedLoader().open(parentMetrics);
    }

    @Override
    public void close() throws Exception {
        getWrappedLoader().close();
    }
}
