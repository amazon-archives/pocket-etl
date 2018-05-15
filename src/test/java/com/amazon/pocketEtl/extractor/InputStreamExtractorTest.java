/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.prefs.BackingStoreException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void extractorCanExtractValues() throws BackingStoreException {
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenReturn("one", "two", "three").thenThrow(new RuntimeException("No more values"));

        inputStreamExtractor.open(null);
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("one")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("two")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.of("three")));
        assertThat(inputStreamExtractor.next(), equalTo(Optional.empty()));
    }

    @Test(expected = BackingStoreException.class)
    public void nextThrowsBackingStoreExceptionIfIOExceptionOccursWhileReadingStream() throws Exception {
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenThrow(new RuntimeException("Boom"));

        inputStreamExtractor.open(null);
        inputStreamExtractor.next();
    }

    @Test(expected = IllegalStateException.class)
    public void nextThrowsIllegalStateExceptionIfNotOpened() throws Exception {
        inputStreamExtractor.next();
    }

    @Test(expected = RuntimeException.class)
    public void openThrowsRuntimeExceptionIfRuntimeExceptionThrownByInputStreamMapper() throws Exception {
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