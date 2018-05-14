/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Extractor;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;
import java.util.prefs.BackingStoreException;

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
    public void callingNextBeforeOpenThrowsIllegalStateException() throws Exception {
        IterableExtractor.of(ImmutableList.of("1", "2", "3")).next();
    }

    @Test(expected = BackingStoreException.class)
    public void nextThrowsRuntimeExceptionThrowsAsBackingStoreException() throws Exception {
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
