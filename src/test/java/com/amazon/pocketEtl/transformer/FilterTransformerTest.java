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

package com.amazon.pocketEtl.transformer;

import com.amazon.pocketEtl.lookup.Lookup;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.function.BiPredicate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FilterTransformerTest {

    private static final String SAMPLE_STRING_ONE = "sampleStringOne";
    private static final String SAMPLE_STRING_TWO = "sampleStringTwo";

    @Mock
    private BiPredicate<String, Lookup<String, String>> mockFilter;

    @Mock
    private Lookup<String, String> mockLookup;

    private FilterTransformer<String> filterTransformer;

    @Before
    public void stubFilterAndLookup() {
        when(mockFilter.test(anyString(), eq(mockLookup))).thenReturn(false);
        when(mockFilter.test(SAMPLE_STRING_ONE, mockLookup)).thenReturn(true);
    }

    @Before
    public void initializeFilterTransformer() {
        filterTransformer = new FilterTransformer<>(mockFilter, mockLookup);
    }

    @Test
    public void transformReturnsListOfObject() throws Exception {
        List<String> transformedObjects = filterTransformer.transform(SAMPLE_STRING_ONE);
        assertThat(transformedObjects, equalTo(ImmutableList.of(SAMPLE_STRING_ONE)));
    }

    @Test
    public void transformReturnsEmptyList() throws Exception {
        List<String> transformedObjects = filterTransformer.transform(SAMPLE_STRING_TWO);
        assertThat(transformedObjects, equalTo(ImmutableList.of()));
    }
}
