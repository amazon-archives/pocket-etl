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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(MockitoJUnitRunner.class)
public class S3BufferedInputStreamTest {
    private final static String S3_BUCKET = "s3Bucket";
    private final static String S3_KEY = "s3Key";

    @Mock
    private AmazonS3 mockAmazonS3;

    @Mock
    private S3ObjectInputStream mockS3ObjectInputStream;

    @Mock
    private InputStream mockCachedInputStream;

    @Mock
    private S3Object mockS3Object;

    private S3BufferedInputStream s3BufferedInputStream;

    @Before
    public void constructS3InputStreamFactory() {
        s3BufferedInputStream = new S3BufferedInputStream(S3_BUCKET, S3_KEY, mockAmazonS3, inputStream -> {
            if (inputStream != mockS3ObjectInputStream) {
                fail("The wrong InputStream was cached");
            }

            return mockCachedInputStream;
        });
    }

    @Before
    public void stubAmazonS3() {
        when(mockAmazonS3.getObject(anyString(), anyString())).thenReturn(mockS3Object);
        when(mockS3Object.getObjectContent()).thenReturn(mockS3ObjectInputStream);
    }

    @Test
    public void supplierHasNoInteractionWithS3WhenBuilding() {
        S3BufferedInputStream.supplierOf(S3_BUCKET, S3_KEY).withClient(mockAmazonS3).get();

        verifyNoMoreInteractions(mockAmazonS3);
    }

    @Test
    public void supplierConstructsS3BufferedInputStreamAndReadsFromS3() throws Exception {
        S3BufferedInputStream supplierConstructedObject = S3BufferedInputStream.supplierOf(S3_BUCKET, S3_KEY)
                .withClient(mockAmazonS3).get();
        when(mockS3ObjectInputStream.read(any())).thenReturn(-1);

        supplierConstructedObject.close();

        verify(mockS3ObjectInputStream).read(any());
    }

    @Test
    public void doingNothingDoesNotLoadFileFromS3() {
        verifyNoMoreInteractions(mockAmazonS3);
    }

    @Test
    public void readCallsGetObjectOnS3ClientInputStream() throws Exception {
        s3BufferedInputStream.read();
        verify(mockAmazonS3).getObject(eq(S3_BUCKET), eq(S3_KEY));
    }

    @Test
    public void closeThrowsExceptionFromS3ClientGetObject() throws Exception {
        AmazonServiceException expectedException = new AmazonServiceException("Fake AWS exception");
        when(mockAmazonS3.getObject(anyString(), anyString())).thenThrow(expectedException);

        try {
            s3BufferedInputStream.read();
            fail("Exception should have been thrown");
        } catch (AmazonServiceException caughtException) {
            assertThat(caughtException, sameInstance(expectedException));
        }
    }

    @Test(expected = IOException.class)
    public void readThrowsIOExceptionWhenCachedInputStreamThrowsIOException() throws Exception {
        when(mockCachedInputStream.read()).thenThrow(new IOException("Test exception"));
        s3BufferedInputStream.read();
    }

    @Test
    public void readThrowsExceptionFromS3ClientGetObjectContent() throws IOException {
        AmazonServiceException expectedException = new AmazonServiceException("Fake AWS exception");
        when(mockS3Object.getObjectContent()).thenThrow(expectedException);

        try {
            s3BufferedInputStream.read();
            fail("Exception should have been thrown");
        } catch (AmazonServiceException caughtException) {
            assertThat(caughtException, sameInstance(expectedException));
        }
    }

    @Test
    public void readCallsWrappedInputStream() throws Exception {
        when(mockCachedInputStream.read()).thenReturn(123);

        int result = s3BufferedInputStream.read();

        assertThat(result, is(123));
        verify(mockCachedInputStream).read();
    }

    @Test
    public void readWithBufferCallsWrappedInputStream() throws Exception {
        byte[] buffer = new byte[3];
        when(mockCachedInputStream.read(any())).thenReturn(123);

        int result = s3BufferedInputStream.read(buffer);

        assertThat(result, is(123));
        verify(mockCachedInputStream).read(buffer);
    }

    @Test
    public void readWithBufferAndLimitCallsWrappedInputStream() throws Exception {
        byte[] buffer = new byte[3];
        when(mockCachedInputStream.read(any(), anyInt(), anyInt())).thenReturn(123);

        int result = s3BufferedInputStream.read(buffer, 123, 456);

        assertThat(result, is(123));
        verify(mockCachedInputStream).read(buffer, 123, 456);
    }

    @Test
    public void skipCallsWrappedInputStream() throws Exception {
        when(mockCachedInputStream.skip(anyLong())).thenReturn(123L);

        long result = s3BufferedInputStream.skip(123L);

        assertThat(result, is(123L));
        verify(mockCachedInputStream).skip(123L);
    }

    @Test
    public void availableCallsWrappedInputStream() throws Exception {
        when(mockCachedInputStream.available()).thenReturn(123);

        int result = s3BufferedInputStream.available();

        assertThat(result, is(123));
        verify(mockCachedInputStream).available();
    }

    @Test
    public void closeCallsWrappedInputStream() throws Exception {
        s3BufferedInputStream.close();
        verify(mockCachedInputStream).close();
    }

    @Test
    public void markSupportedIsFalse()
    {
        assertThat(s3BufferedInputStream.markSupported(), is(false));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void markThrowsUnsupportedOperationException() {
        s3BufferedInputStream.mark(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void resetThrowsUnsupportedOperationException() {
        s3BufferedInputStream.reset();
    }
}
