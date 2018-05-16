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
