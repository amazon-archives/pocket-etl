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

package com.amazon.pocketEtl.transformer.filter;

import com.amazon.pocketEtl.lookup.Lookup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContainsFilterTest {
    private final static String TEST_STRING_ONE = "TestStringOne";
    private final static String TEST_STRING_TWO = "TestStringTwo";

    @Mock
    private Lookup<String, String> mockLookup;

    private ContainsFilter<String> containsFilter = new ContainsFilter<>();

    @Before
    public void stubMockLookup() {
        when(mockLookup.get(anyString())).thenReturn(Optional.empty());
        when(mockLookup.get(eq(TEST_STRING_ONE))).thenReturn(Optional.of(TEST_STRING_ONE));
    }

    @Test
    public void testReturnsTrueIfObjectIsFoundInLookup() {
        boolean result = containsFilter.test(TEST_STRING_ONE, mockLookup);

        assertThat(result, is(true));
    }

    @Test
    public void testReturnsFalseIfObjectIsNotFoundInLookup() {
        boolean result = containsFilter.test(TEST_STRING_TWO, mockLookup);

        assertThat(result, is(false));
    }
}
