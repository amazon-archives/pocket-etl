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
