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

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class IterableExtractorTest extends EtlTestBase {
    @Test
    public void iteratorExtractorExtractsAndIterates() throws Exception {
        try (Extractor<String> extractor = IterableExtractor.of(ImmutableList.of("1", "2", "3"))) {
            extractor.open(mockMetrics);
            assertThat(extractor.next(), equalTo(Optional.of("1")));
            assertThat(extractor.next(), equalTo(Optional.of("2")));
            assertThat(extractor.next(), equalTo(Optional.of("3")));
            assertThat(extractor.next(), equalTo(Optional.empty()));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void callingNextBeforeOpenThrowsIllegalStateException() {
        IterableExtractor.of(ImmutableList.of("1", "2", "3")).next();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void nextThrowsRuntimeExceptionThrowsAsUnrecoverableStreamFailureException() {
        Iterable mockIterable = mock(Iterable.class);
        Iterator mockIterator = mock(Iterator.class);
        when(mockIterable.iterator()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(new RuntimeException("Test exception"));

        Extractor iterableExtractor = IterableExtractor.of(mockIterable);
        iterableExtractor.open(null);
        iterableExtractor.next();
    }
}
