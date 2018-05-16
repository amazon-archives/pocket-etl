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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Iterator;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3BufferedExtractorTest {
    private final static String S3_BUCKET = "s3-bucket";
    private final static String S3_KEY = "s3-key";

    @Mock
    private InputStreamMapper<String> mockInputStreamMapper;
    @Mock
    private AmazonS3 mockAmazonS3;
    @Mock
    private S3Object mockS3Object;
    @Mock
    private S3ObjectInputStream mockS3ObjectInputStream;

    private final Iterator<String> testIterator = ImmutableList.of("one", "two", "three").iterator();

    private S3BufferedExtractor<String> s3BufferedExtractor;

    @Before
    public void createS3BufferedExtractor() {
        s3BufferedExtractor = S3BufferedExtractor.supplierOf(S3_BUCKET, S3_KEY, mockInputStreamMapper)
                .withClient(mockAmazonS3)
                .get();
    }

    @Before
    public void stubAmazonS3() throws Exception {
        when(mockAmazonS3.getObject(anyString(), anyString())).thenReturn(mockS3Object);
        when(mockS3Object.getObjectContent()).thenReturn(mockS3ObjectInputStream);
        when(mockS3ObjectInputStream.read(any())).thenReturn(-1);
    }

    @Before
    public void stubIterator() {
        when(mockInputStreamMapper.apply(any())).thenReturn(testIterator);
    }

    @Test
    public void supplierConstructsAnInputStreamExtractorThatReadsFromS3() throws Exception {
        s3BufferedExtractor.open(null);
        s3BufferedExtractor.next();
        s3BufferedExtractor.close();

        verify(mockS3ObjectInputStream).read(any());
    }

    @Test
    public void supplierConstructsAnInputStreamExtractorThatExtractsValues() throws Exception {
        s3BufferedExtractor.open(null);

        assertThat(s3BufferedExtractor.next(), equalTo(Optional.of("one")));
        assertThat(s3BufferedExtractor.next(), equalTo(Optional.of("two")));
        assertThat(s3BufferedExtractor.next(), equalTo(Optional.of("three")));
        assertThat(s3BufferedExtractor.next(), equalTo(Optional.empty()));

        s3BufferedExtractor.close();

        verify(mockS3ObjectInputStream).read(any());
    }
}
