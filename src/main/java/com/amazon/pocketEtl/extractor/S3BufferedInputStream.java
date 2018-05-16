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

import com.amazon.pocketEtl.common.ThrowingFunction;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * InputStream implementation that can be used to read a file in S3. The entire file will be read into memory when the
 * S3BufferedInputStream is first used, this is to avoid the risk of the stream being closed by the remote service before
 * it has been fully processed. This approach will be problematic with extremely large files for obvious reasons, but it
 * is good for handling sensitive data because nothing is written to disk.
 * * This has been tested to also work with SSE-KMS encrypted files.
 * * Mark and reset are not supported by this implementation, and attempts to use them will throw an exception.
 * * If a pre-built s3 client is not specified when constructing this object then the default client builder will be used.
 *
 * Example usage:
 * S3BufferedInputStream.supplierOf("MyBucket", "MyFile.csv").get();
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class S3BufferedInputStream extends WrappedInputStream {
    private final String s3Bucket;
    private final String s3Key;
    private final AmazonS3 amazonS3;
    private final ThrowingFunction<InputStream, InputStream, IOException> inputStreamCachingFunction;

    private final static ThrowingFunction<InputStream, InputStream, IOException> DEFAULT_CACHING_FUNCTION = (inputStream ->
            new ByteArrayInputStream(IOUtils.toByteArray(inputStream)));

    private InputStream wrappedInputStream = null;

    /**
     * Creates a new supplier for S3BufferedInputStream objects. This is the only way to create an S3BufferedInputStream.
     *
     * @param bucket S3 bucket name.
     * @param key    S3 object key.
     * @return An S3BufferedInputStreamSupplier that is not associated with any S3 client.
     */
    @Nonnull
    public static S3BufferedInputStreamSupplier supplierOf(@Nonnull String bucket, @Nonnull String key) {
        return new S3BufferedInputStreamSupplier(bucket, key, null);
    }

    @Override
    protected InputStream getWrappedInputStream() throws IOException {
        if (wrappedInputStream == null) {
            S3Object s3Object = amazonS3.getObject(s3Bucket, s3Key);
            wrappedInputStream = inputStreamCachingFunction.apply(s3Object.getObjectContent());
        }

        return wrappedInputStream;
    }

    /**
     * This class supplies S3BufferedInputStream objects with a specific destination and s3 client. If an S3 client is
     * not provided, then the default S3 client builder will be used.
     *
     * Example usage:
     * S3BufferedInputStream.supplierOf("myBucket", "/path/to/my/file").get();
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class S3BufferedInputStreamSupplier implements Supplier<InputStream> {
        private final String s3Bucket;
        private final String s3Key;
        private final AmazonS3 amazonS3;

        /**
         * Creates a new provider based on the current one that is associated with a specific AmazonS3 client object.
         *
         * @param amazonS3Client An AmazonS3 client object or null to use the default client.
         * @return A new S3BufferedInputStream.
         */
        @Nonnull
        public S3BufferedInputStreamSupplier withClient(@Nullable AmazonS3 amazonS3Client) {
            return new S3BufferedInputStreamSupplier(s3Bucket, s3Key, amazonS3Client);
        }

        /**
         * Reads the file from S3 and returns an InputStream for the contents of the file. This has been tested to also work
         * with SSE-KMS encrypted files.
         *
         * @return an open InputStream for the contents of the file in S3.
         */
        @Override
        @Nonnull
        public S3BufferedInputStream get() {
            AmazonS3 s3Client = (amazonS3 == null) ? AmazonS3Client.builder().build() : amazonS3;
            return new S3BufferedInputStream(s3Bucket, s3Key, s3Client, DEFAULT_CACHING_FUNCTION);
        }
    }
}