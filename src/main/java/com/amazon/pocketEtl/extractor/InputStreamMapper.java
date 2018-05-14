/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Functional interface that maps an InputStream to an Iterator of a specific type. Used by InputStreamExtractor.
 * @param <T> Type of object being mapped to.
 */
@SuppressWarnings("WeakerAccess")
@FunctionalInterface
public interface InputStreamMapper<T> extends Function<InputStream, Iterator<T>> {
}
