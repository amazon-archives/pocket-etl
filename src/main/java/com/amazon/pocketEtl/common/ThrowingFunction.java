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
