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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

@RunWith(MockitoJUnitRunner.class)
public class InputStreamExtractorTest {
    @Mock
    private InputStream mockInputStream;

    @Mock
    private Iterator<String> mockIterator;

    @Mock
    private InputStreamMapper<String> mockInputStreamMapper;

    private InputStreamExtractor<String> inputStreamExtractor;

    @Before
    public void initializeMocks() {
        when(mockInputStreamMapper.apply(mockInputStream)).thenReturn(mockIterator);

        inputStreamExtractor =
                (InputStreamExtractor<String>)InputStreamExtractor.of(() -> mockInputStream, mockInputStreamMapper);
    }

    @Test
    public void extractorCanExtractValues() {
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenReturn("one", "two", "three").thenThrow(new RuntimeException("No more values"));

        inputStreamExtractor.open(null);
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("one")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("two")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("three")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.empty()));
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void nextThrowsUnrecoverableStreamFailureExceptionIfIOExceptionOccursWhileReadingStream() {
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenThrow(new RuntimeException("Boom"));

        inputStreamExtractor.open(null);
        inputStreamExtractor.next();
    }

    @Test(expected = IllegalStateException.class)
    public void nextThrowsIllegalStateExceptionIfNotOpened() {
        inputStreamExtractor.next();
    }

    @Test(expected = RuntimeException.class)
    public void openThrowsRuntimeExceptionIfRuntimeExceptionThrownByInputStreamMapper() {
        when(mockInputStreamMapper.apply(mockInputStream)).thenThrow(new RuntimeException("Bad AWS exception"));
        inputStreamExtractor.open(null);
    }

    @Test
    public void closeClosesInputStream() throws Exception {
        inputStreamExtractor.open(null);
        inputStreamExtractor.close();
        verify(mockInputStream).close();
    }
}
