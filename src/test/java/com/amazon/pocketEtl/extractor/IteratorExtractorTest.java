/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Extractor;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class IteratorExtractorTest extends EtlTestBase {
    @Test
    public void iteratorExtractorExtractsAndIterates() throws Exception {
        try (Extractor<Integer> extractor = IteratorExtractor.of(ImmutableList.of(1, 2, 3).iterator())) {
            extractor.open(mockMetrics);
            assertThat(extractor.next(), equalTo(Optional.of(1)));
            assertThat(extractor.next(), equalTo(Optional.of(2)));
            assertThat(extractor.next(), equalTo(Optional.of(3)));
            assertThat(extractor.next(), equalTo(Optional.empty()));
        }
    }
}
