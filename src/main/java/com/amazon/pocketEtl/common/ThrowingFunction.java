/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.common;

/**
 * Simple functional interface for a method that takes a single argument, returns a result and can throw an exception.
 * @param <T> Same as Function : Argument class type
 * @param <R> Same as Function : Result class type
 * @param <E> Checked exception type that the function can throw.
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Throwable> {
    /**
     * A method that takes a single argument, returns a result and can throw an exception.
     * @param t Function argument.
     * @return Function result.
     * @throws E This function can throw this exception.
     */
    R apply(T t) throws E;
}
