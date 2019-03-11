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

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3FastLoaderTest extends EtlTestBase {

    private static final Charset UTF8_CHARSET = Charset.forName("UTF8");
    private static final String KMS_SSE_ENCRYPTION_TYPE = "aws:kms";
    private static final String S3_BUCKET = "aS3Bucket";
    private static final String S3_PREFIX = "aGoodPrefix";
    private static final String S3_SUFFIX = ".ext";
    private static final String KMS_KEY = "KmsKey";
    private static final String S3_WRITE_SUCCESS_METRIC_KEY = "S3FastLoader.success";
    private static final String S3_WRITE_FAIL_METRIC_KEY = "S3FastLoader.failure";
    private static final int BUFFER_SIZE = 10;
    private S3FastLoader<Object> s3FastLoader;
    private List<List<Byte>> s3StreamCapture = new ArrayList<>();

    @Mock
    private AmazonS3 s3Client;

    @Mock
    private Supplier<StringSerializer<Object>> mockStringSerializerProvider;

    @Mock
    private StringSerializer<Object> mockStringSerializer;

    @Mock
    private EtlMetrics mockMetrics;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor;

    @Before
    public void buildS3Writer() {
        s3FastLoader = S3FastLoader.supplierOf(S3_BUCKET, mockStringSerializerProvider)
                .withClient(s3Client)
                .withS3PartFileKeyGenerator(($, partNum) -> S3_PREFIX + "/" + partNum + S3_SUFFIX)
                .withMaxPartFileSizeInBytes(BUFFER_SIZE)
                .withSSEKmsArn(KMS_KEY)
                .get();
    }

    @Before
    public void initializeMetrics() {
        when(mockMetrics.createChildMetrics()).thenReturn(mockMetrics);
    }

    @Before
    public void initializeSerializer() {
        when(mockStringSerializerProvider.get()).thenReturn(mockStringSerializer);
    }

    @Before
    public void initializeS3StreamCapture() {
        when(s3Client.putObject(any(PutObjectRequest.class))).thenAnswer(invocation -> {
            PutObjectRequest putObjectRequest = (PutObjectRequest) invocation.getArguments()[0];
            InputStream inputStream = putObjectRequest.getInputStream();
            s3StreamCapture.add(byteArrayToList(toByteArray(inputStream)));
            return new PutObjectResult();
        });
    }

    @Test
    public void writeCallsS3WithCorrectStreamWhenFirstWriteIsEqualToBufferSize() {
        String string1 = "1234567890";
        String string2 = "ABCDEFGH";

        testS3LoaderWithStrings(string1, string2); // smaller than bufferMax, larger than remainingSpace

        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class));
        List<Byte> expectedBytes = byteArrayToList(string1.getBytes(UTF8_CHARSET));
        assertThat(s3StreamCapture, equalTo(ImmutableList.of(expectedBytes)));
    }

    @Test
    public void writeToS3GetsANewSerializerInstance() {
        String string1 = "1234567890";
        String string2 = "ABCDEFGH";

        testS3LoaderWithStrings(string1, string2); // smaller than bufferMax, larger than remainingSpace

        verify(mockStringSerializerProvider, times(2)).get();
    }

    @Test
    public void writeCallsS3WithCorrectBucket() {
        testS3LoaderWithStrings("1234567890ABC");

        verify(s3Client, times(1)).putObject(putObjectRequestArgumentCaptor.capture());
        final PutObjectRequest request = putObjectRequestArgumentCaptor.getValue();

        assertThat(request.getBucketName(), equalTo(S3_BUCKET));
    }

    @Test
    public void writeCallsS3WithKmsSseEncryption() {
        testS3LoaderWithStrings("123456789ABCDEF");

        verify(s3Client, times(1)).putObject(putObjectRequestArgumentCaptor.capture());

        final PutObjectRequest request = putObjectRequestArgumentCaptor.getValue();

        SSEAwsKeyManagementParams SSEParams = request.getSSEAwsKeyManagementParams();
        assertThat(SSEParams.getEncryption(), equalTo(KMS_SSE_ENCRYPTION_TYPE));
        assertThat(SSEParams.getAwsKmsKeyId(), equalTo(KMS_KEY));
    }

    @Test
    public void writeCallsS3WithContentStreamLength() {
        testS3LoaderWithStrings("123456789ABCDEF");

        verify(s3Client, times(1)).putObject(putObjectRequestArgumentCaptor.capture());
        final PutObjectRequest request = putObjectRequestArgumentCaptor.getValue();

        assertThat(request.getMetadata().getContentLength(), equalTo(15L));
    }

    @Test
    public void writeDoesNotWriteToS3IfLessThanRemainingBuffer() {
        testS3LoaderWithStrings("a");

        verifyNoMoreInteractions(s3Client);
    }

    @Test
    public void writeFlushesIfObjectLargerThanRemainingBuffer() {
        testS3LoaderWithStrings("12345", "6789ABC"); // smaller than bufferMax, larger than remainingSpace

        verify(s3Client, times(1)).putObject(any());
    }

    @Test
    public void writeFlushesAndWritesIfObjectSizeLargerThanTotalBufferSize() {
        String string1 = "1";
        String string2 = "123456789ABCDEF";

        testS3LoaderWithStrings(string1, string2);

        verify(s3Client, times(2)).putObject(putObjectRequestArgumentCaptor.capture());

        List<Byte> expectedBytes1 = byteArrayToList(string1.getBytes(UTF8_CHARSET));
        List<Byte> expectedBytes2 = byteArrayToList(string2.getBytes(UTF8_CHARSET));

        assertThat(s3StreamCapture, equalTo(ImmutableList.of(expectedBytes1, expectedBytes2)));
    }


    @Test
    public void closeFlushesBuffer() throws Exception {
        String shouldBeWritten = "12345";
        String wouldOverflowBufferIfNotFlushed = "67890ABC";

        testS3LoaderWithStrings(shouldBeWritten);
        s3FastLoader.close();
        // if the buffer isn't being cleared correctly, two calls will be made
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class));

        testS3LoaderWithStrings(wouldOverflowBufferIfNotFlushed);
        verifyNoMoreInteractions(s3Client);

        List<Byte> expectedBytes = byteArrayToList(shouldBeWritten.getBytes(UTF8_CHARSET));
        assertThat(s3StreamCapture, equalTo(ImmutableList.of(expectedBytes)));
    }

    @Test
    public void s3WritesHaveCorrectKeysWithIncrementingSequenceNumber() {
        testS3LoaderWithStrings("123456789ABCDEF", "123456789ABCDEF", "123456789ABCDEF");
        verify(s3Client, times(3)).putObject(putObjectRequestArgumentCaptor.capture());

        List<PutObjectRequest> requests = putObjectRequestArgumentCaptor.getAllValues();
        assertThat(requests.get(0).getKey(), equalTo(S3_PREFIX + "/1" + S3_SUFFIX));
        assertThat(requests.get(1).getKey(), equalTo(S3_PREFIX + "/2" + S3_SUFFIX));
        assertThat(requests.get(2).getKey(), equalTo(S3_PREFIX + "/3" + S3_SUFFIX));
    }

    @Test
    public void writeWithExceptionThrowsUnrecoverableStreamFailureException() {
        AmazonClientException s3Exception = new AmazonClientException("oh no!");
        reset(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class))).thenThrow(s3Exception);

        try {
            testS3LoaderWithStrings("123456789ABCDEF");
            fail("Expected UnrecoverableStreamFailureException");
        } catch (UnrecoverableStreamFailureException e) {
            assertThat(e.getCause(), is(s3Exception));
        }

        verify(s3Client).putObject(any());
    }

    @Test
    public void closeWithExceptionThrowsUnrecoverableStreamFailureException() throws Exception {
        AmazonClientException s3Exception = new AmazonClientException("oh no!");
        reset(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class))).thenThrow(s3Exception);
        testS3LoaderWithStrings("123456");

        try {
            s3FastLoader.close();
            fail("Expected UnrecoverableStreamFailureException");
        } catch (UnrecoverableStreamFailureException e) {
            assertThat(e.getCause(), is(s3Exception));
        }

        verify(s3Client).putObject(any());
    }

    @Test
    public void writeToS3OperationEmitsMetricsOnSuccess() {
        testS3LoaderWithStrings("123456789ABCDEF");

        verify(mockMetrics).addCount(S3_WRITE_SUCCESS_METRIC_KEY, 1.0);
        verify(mockMetrics).addCount(S3_WRITE_FAIL_METRIC_KEY, 0.0);
    }

    @Test
    public void writeToS3OperationEmitsMetricsOnFail() {
        AmazonClientException s3Exception = new AmazonClientException("oh no!");
        reset(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class))).thenThrow(s3Exception);

        try {
            testS3LoaderWithStrings("123456789ABCDEF");
            fail("RuntimeException should have been thrown");
        } catch (RuntimeException ignored) {

        }

        verify(mockMetrics).addCount(S3_WRITE_SUCCESS_METRIC_KEY, 0.0);
        verify(mockMetrics).addCount(S3_WRITE_FAIL_METRIC_KEY, 1.0);
    }

    /**
     * Reads and returns the rest supplierOf the given input stream as a byte array,
     * closing the input stream afterwards.
     */
    public static byte[] toByteArray(InputStream is) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] b = new byte[BUFFER_SIZE + 1];
            int n;
            while ((n = is.read(b)) != -1) {
                output.write(b, 0, n);
            }
            return output.toByteArray();
        }
    }

    private static List<Byte> byteArrayToList(byte[] byteArray) {
        List<Byte> byteList = new ArrayList<>();

        for (Byte b : byteArray) {
            byteList.add(b);
        }

        return byteList;
    }

    private void testS3LoaderWithStrings(String firstString, String ...stringsToTest) {
        when(mockStringSerializer.apply(any())).thenReturn(firstString, stringsToTest);
        s3FastLoader.open(mockMetrics);
        IntStream.rangeClosed(0, stringsToTest.length).forEach(s3FastLoader::load);
    }
}
