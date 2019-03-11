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

import static lombok.AccessLevel.PROTECTED;

import java.util.Optional;

import javax.annotation.Nullable;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.RequiredArgsConstructor;

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
    public Optional<T> next() throws UnrecoverableStreamFailureException {
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
