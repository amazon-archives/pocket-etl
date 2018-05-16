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

import com.amazon.pocketEtl.Extractor;
import com.amazonaws.services.s3.AmazonS3;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * An extractor implementation that reads an entire file from S3 and stores it in memory before any objects are returned
 * by next(). As no temporary data is written to disk, this extractor can be used to handle sensitive data that is
 * encrypted in S3 and should not be stored unencrypted at rest.
 *
 * An InputStreamMapper object must be supplied so that the contents of the file can be converted into the Java objects
 * you are trying to extract as. An example is the CSVInputStreamMapper that gives this extractor the capability of
 * reading CSV files.
 *
 * @param <T> The type of object being extracted.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class S3BufferedExtractor<T> extends WrappedExtractor<T> {
    private final Extractor<T> wrappedExtractor;

    /**
     * Creates a factory that can manufacture S3BufferedExtractor objects on demand with a specific configuration.
     *
     * @param s3Bucket the S3 bucket name to read the data from.
     * @param s3Key the S3 key that identifies the object in S3 to read the data from (aka. filename).
     * @param inputStreamMapper an InputStreamMapper object which will be used to read and deserialize the data in the
     *                          S3 object into extracted java objects.
     * @param <T> the type of object being extracted.
     * @return An S3BufferedExtractorSupplier object that can be configured and create S3BufferedExtractor objects from.
     */
    public static <T> S3BufferedExtractorSupplier<T> supplierOf(String s3Bucket, String s3Key,
                                                                InputStreamMapper<T> inputStreamMapper) {
        return new S3BufferedExtractorSupplier<>(s3Bucket, s3Key, inputStreamMapper, null);
    }

    // Simple wrapping for the real extractor, just uses the stored object.
    @Override
    protected Extractor<T> getWrappedExtractor() {
        return wrappedExtractor;
    }

    /**
     * A factory that supplies S3BufferedExtractor objects. Allows a pre-constructed AmazonS3 client object to be used,
     * otherwise one will be built for you using the AWS default client builder.
     *
     * @param <T> The type of object being extracted.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class S3BufferedExtractorSupplier<T> implements Supplier<S3BufferedExtractor<T>> {
        private final String s3Bucket;
        private final String s3Key;
        private final InputStreamMapper<T> inputStreamMapper;
        private final AmazonS3 amazonS3;

        /**
         * Creates a new provider based on the current one that is associated with a specific AmazonS3 client object.
         *
         * @param amazonS3Client An AmazonS3 client object or null to use the default client.
         * @return A new S3BufferedInputStream.
         */
        @Nonnull
        public S3BufferedExtractorSupplier<T> withClient(@Nullable AmazonS3 amazonS3Client) {
            return new S3BufferedExtractorSupplier<>(s3Bucket, s3Key, inputStreamMapper, amazonS3Client);
        }

        /**
         * Get a constructed instance of an S3BufferedExtractor initialized with the parameters stored on the supplier
         * object.
         *
         * @return an initialized instance of an S3BufferedExtractor.
         */
        @Override
        public S3BufferedExtractor<T> get() {
            S3BufferedInputStream.S3BufferedInputStreamSupplier s3BufferedInputStreamSupplier =
                    S3BufferedInputStream.supplierOf(s3Bucket, s3Key);

            if (amazonS3 != null) {
                s3BufferedInputStreamSupplier = s3BufferedInputStreamSupplier.withClient(amazonS3);
            }

            return new S3BufferedExtractor<>(InputStreamExtractor.of(s3BufferedInputStreamSupplier, inputStreamMapper));
        }
    }
}
