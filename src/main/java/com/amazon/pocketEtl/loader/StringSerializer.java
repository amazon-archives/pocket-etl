/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.loader;

import java.util.function.Function;

/**
 * Functional interface for a function that takes an object of a specific type and serializes it into a string. Used
 * by loaders that write objects as strings such as S3FastLoader.
 * @param <T> The type of object being serialized.
 */
@SuppressWarnings("WeakerAccess")
@FunctionalInterface
public interface StringSerializer<T> extends Function<T, String> {
}
