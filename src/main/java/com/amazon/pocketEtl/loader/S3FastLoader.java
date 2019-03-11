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
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Loader implementation that loads data in serial form to multiple S3 part files. This class is optimized for throughput
 * so it is deliberately not synchronized and is not thread-safe. The best way to use this class is to construct a
 * Supplier using the supplierOf static method and passing that supplier into a ParallelLoader which guarantees only a
 * single thread will access a single constructed instance of the S3Loader. Eg:
 *
 * ParallelLoader.of(S3FastLoader.supplierOf(...))
 *
 * The implementation of this loader buffers data in memory until it is ready to write a complete part file in S3 and
 * then writes the entire part file at once and clears the buffer to start loading more data.
 *
 * A serialization function that can convert the data objects into strings is required for this Loader to function.
 *
 * @param <T> The type of objects being loaded.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class S3FastLoader<T> implements Loader<T> {
    private final static Logger logger = getLogger(S3FastLoader.class);

    // Default part-size is 128 MiB
    private final static int DEFAULT_MAX_PARTFILE_SIZE_IN_BYTES = 1024 * 1024 * 128;

    private final static String SUCCESS_METRIC_KEY = "S3FastLoader.success";
    private final static String FAILURE_METRIC_KEY = "S3FastLoader.failure";
    private final static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private final AmazonS3 amazonS3;
    private final String s3Bucket;
    private final Integer maxPartFileSizeInBytes;
    private final String sseKmsArn;
    private final Function<Integer, String> s3PartFileKeyGenerator;
    private final Supplier<StringSerializer<T>> stringSerializerProvider;

    private EtlMetrics parentMetrics;
    private ByteBuffer buffer = null;
    private StringSerializer<T> stringSerializer = null;
    private int fileSequenceNumber = 0;

    /**
     * Constructs a new S3FastLoaderSupplier which will supply sequenced instances of S3FastLoader objects that can be used
     * in parallel as they will write to different keys in the S3 bucket.
     *
     * Example usage:
     * S3FastLoader.supplierOf("myBucket", CsvStringSerializer.supplierOf(MyObject.class))
     *
     * By default the files will be named thus:-
     * PocketETL/01/part-00001
     * PocketETL/01/part-00002
     * PocketETL/02/part-00001
     * ...etc
     *
     * In this example the first loader provided has written two part files, and the second loader provided has just
     * written a single one.
     *
     * To implement this same functionality yourself you would supply a custom s3PartFileKeyGenerator:
     * (sequenceNum, partNum) -> String.format("PocketETL/%02d/part-%05d", sequenceNum, partNum)
     *
     * @param s3Bucket The S3 bucket to write the data into.
     * @param stringSerializerSupplier A function that turns an object to load into a serial string.
     * @param <T> The type supplierOf object being loaded.
     * @return A newly constructed S3FastLoaderSupplier object.
     */
    public static <T> S3FastLoaderSupplier<T> supplierOf(String s3Bucket, Supplier<StringSerializer<T>> stringSerializerSupplier) {
        return new S3FastLoaderSupplier<>(null, null, null, s3Bucket, null, stringSerializerSupplier);
    }

    /**
     * Loads the next object into the serial buffer. If the buffer is going to exceed the maximum buffer size then
     * the entire buffer will be written to a new part file in S3, and the object being loaded will be added to a
     * new buffer.
     * @param objectToLoad The object to be loaded.
     */
    @Override
    public void load(T objectToLoad) {
        String serializedObject = stringSerializer.apply(objectToLoad);
        byte[] serializedObjectBytes = serializedObject.getBytes(DEFAULT_CHARSET);

        if (buffer.remaining() < serializedObjectBytes.length) {
            flushBuffer();
            serializedObject = stringSerializer.apply(objectToLoad);
            serializedObjectBytes = serializedObject.getBytes(DEFAULT_CHARSET);
        }

        if (buffer.remaining() < serializedObjectBytes.length) {
            writeBufferToS3(serializedObjectBytes, serializedObjectBytes.length);
        } else {
            buffer.put(serializedObjectBytes);
        }
    }

    /**
     * Prepares the loader to start accepting objects to load. Allocates the memory buffer.
     * @param parentMetrics An EtlMetrics object to attach any child threads created by load() to
     */
    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "S3FastLoader.open")) {
            this.parentMetrics = parentMetrics;
            buffer = ByteBuffer.allocate(getMaxPartFileSizeInBytes());
            stringSerializer = stringSerializerProvider.get();
        }
    }

    /**
     * Will flush any data currently buffered to S3 and close the loader.
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "S3FastLoader.close")) {
            flushBuffer();
        }
    }

    private void flushBuffer() {
        buffer.flip();

        if (buffer.limit() != 0) {
            writeBufferToS3(buffer.array(), buffer.limit());
        }

        buffer.clear();
        stringSerializer = stringSerializerProvider.get();
    }

    private int getMaxPartFileSizeInBytes() {
        return maxPartFileSizeInBytes == null ? DEFAULT_MAX_PARTFILE_SIZE_IN_BYTES : maxPartFileSizeInBytes;
    }

    private void writeBufferToS3(byte[] toWrite, int limit) {
        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "S3FastLoader.writeToS3")) {
            InputStream inputStream = new ByteArrayInputStream(toWrite, 0, limit);
            String s3Key = s3PartFileKeyGenerator.apply(++fileSequenceNumber);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(limit);
            PutObjectRequest putObjectRequest = new PutObjectRequest(s3Bucket, s3Key, inputStream, metadata);

            if (sseKmsArn != null && !sseKmsArn.isEmpty()) {
                putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(sseKmsArn));
            }

            try {
                amazonS3.putObject(putObjectRequest);
                emitSuccessAndFailureMetrics(scope, true);
            } catch (AmazonClientException e) {
                    logger.error(e);
                    scope.addCounter(e.getClass().getSimpleName(), 1);
                    emitSuccessAndFailureMetrics(scope, false);
                    throw new UnrecoverableStreamFailureException("Exception caught trying to write object to S3: ", e);
            }
        }
    }

    private void emitSuccessAndFailureMetrics(final EtlProfilingScope scope, final boolean isSuccess) {
        scope.addCounter(SUCCESS_METRIC_KEY, isSuccess ? 1 : 0);
        scope.addCounter(FAILURE_METRIC_KEY, isSuccess ? 0 : 1);
    }

    /**
     * A class that supplies S3FastLoader objects on demand. The class keeps a sequence counter which increments each
     * time it constructs a new S3FastLoader and this sequence number can be referenced in the supplied S3 key generator
     * function.
     *
     * To construct an instance of this supplier, use the static method S3FastLoader.supplierOf(...)
     *
     * This class is designed to be used with the ParallelLoader class when you want to load to S3 using multiple parallel
     * loaders using a sequence number to stop the threads interfering with each other when naming files.
     *
     * Example usage:
     * S3Loader.supplierOf("myBucket", CsvStringSerializer.supplierOf(MyObject.class))
     *
     * By default the files will be named thus:-
     * PocketETL/01/part-00001
     * PocketETL/01/part-00002
     * PocketETL/02/part-00001
     * ...etc
     *
     * In this example the first loader provided has written two part files, and the second loader provided has just
     * written a single one.
     *
     * To implement this same functionality yourself you would pass in a custom S3 part file key generator:
     * (sequenceNum, partNum) -> String.format("PocketETL/%02d/part-%05d", sequenceNum, partNum)
     */
    @SuppressWarnings("WeakerAccess")
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static class S3FastLoaderSupplier<T> implements Supplier<Loader<T>> {
        private final String awsKmsArn;
        private final Integer bufferSizeInBytes;
        private final AmazonS3 s3Client;
        private final String s3Bucket;
        private final S3PartFileKeyGenerator s3KeyGenerator;
        private final Supplier<StringSerializer<T>> stringSerializerSupplier;

        private AtomicInteger sequenceCounter = new AtomicInteger(0);

        /**
         * Optional: Enables server-side-encryption on all the loaders supplied by this object using the referenced KMS
         * key.
         * @param awsKmsArn An ARN that identifies the KMS key to use to encrypt the data being written to S3.
         * @return A copy of the current S3FastLoaderSupplier with this property modified.
         */
        public S3FastLoaderSupplier<T> withSSEKmsArn(String awsKmsArn) {
            return new S3FastLoaderSupplier<>(awsKmsArn, bufferSizeInBytes, s3Client, s3Bucket, s3KeyGenerator,
                    stringSerializerSupplier);
        }

        /**
         * Optional: Defines the maximum size of the objects that will be written to S3. If a newly loaded object is going
         * to exceed this limit, a new part file will be started.
         * @param bufferSizeInBytes Maximum file size in bytes.
         * @return A copy of the current S3FastLoaderSupplier with this property modified.
         */
        public S3FastLoaderSupplier<T> withMaxPartFileSizeInBytes(Integer bufferSizeInBytes) {
            return new S3FastLoaderSupplier<>(awsKmsArn, bufferSizeInBytes, s3Client, s3Bucket, s3KeyGenerator,
                    stringSerializerSupplier);
        }

        /**
         * Optional: Specifies an AmazonS3 client used to write the data to S3. If not specified, the default AWS client
         * builder will be used.
         * @param s3Client an Amazon S3 client
         * @return A copy of the current S3FastLoaderSupplier with this property modified.
         */
        public S3FastLoaderSupplier<T> withClient(AmazonS3 s3Client) {
            return new S3FastLoaderSupplier<>(awsKmsArn, bufferSizeInBytes, s3Client, s3Bucket, s3KeyGenerator,
                    stringSerializerSupplier);
        }

        /**
         * Optional: Define the behavior of how part files being written to S3 will be named. The default behavior is
         * PocketETL/{sequence-number}/part-{part-number}.
         * @param s3KeyGenerator A function that given a sequence number and a part number will return an S3 key to use
         *                       to store the object.
         * @return A copy of the current S3FastLoaderSupplier with this property modified.
         */
        public S3FastLoaderSupplier<T> withS3PartFileKeyGenerator(S3PartFileKeyGenerator s3KeyGenerator) {
            return new S3FastLoaderSupplier<>(awsKmsArn, bufferSizeInBytes, s3Client, s3Bucket, s3KeyGenerator,
                    stringSerializerSupplier);
        }

        /**
         * Construct a new loader according to the properties of this provider and increment the sequence counter used to
         * determine S3 key names for future provided loaders.
         * @return A newly constructed S3FastLoader.
         */
        @Override
        @Nonnull
        public S3FastLoader<T> get() {
            final int sequenceNum = sequenceCounter.incrementAndGet();
            Function<Integer,String> keyGeneratorForThread;

            if (s3KeyGenerator == null) {
                keyGeneratorForThread = (part) ->
                        String.format("PocketETL/%02d/part-%05d", sequenceNum, part);
            }
            else {
                keyGeneratorForThread = (part) -> s3KeyGenerator.generateS3Key(sequenceNum, part);
            }

            AmazonS3 effectiveS3Client = (s3Client == null) ? AmazonS3Client.builder().build() : s3Client;

            return new S3FastLoader<>(effectiveS3Client, s3Bucket, bufferSizeInBytes, awsKmsArn, keyGeneratorForThread,
                    stringSerializerSupplier);
        }
    }
}