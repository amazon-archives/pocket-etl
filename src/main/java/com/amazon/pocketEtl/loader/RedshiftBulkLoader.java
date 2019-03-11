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

import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;
import com.amazon.pocketEtl.integration.RedshiftJdbcClient;
import com.amazonaws.services.s3.AmazonS3;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import javax.sql.DataSource;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Loader implementation that loads data to a Redshift table by first writing it to S3 as CSV files, and then invoking
 * COPY on the Redshift instance to have it load the data into a temporary staging table, and then use a query to merge
 * and overwrite that loaded data into the final destination table with a transactional delete and insert query. You can
 * override this behaviour by specifying a load strategy, see {@link RedshiftLoadStrategy}. A random UUID is used as part
 * of the S3 keys writen by this loader, so it is safe to use multiple constructed instances of this loader in parallel,
 * although if they are trying to insert into the same table this could cause database contention.
 *
 * This loader is based on a ParallelLoader that writes to S3 using parallel loaders, so the more threads you give
 * this loader in your EtlStream, the faster it will write its data to S3. The part that copies the data from S3 into
 * Redshift is synchronized, however, and will not be improved by more threads on the EtlStream (although Redshift's
 * own parallelism comes into play at that point).
 *
 * Example usage:
 *
 * {@code
 * RedshiftBulkLoader.supplierOf(Customer.class)
 *   .withS3Bucket("MyBucket")
 *   .withS3Region("us-east-1")
 *   .withRedshiftDataSource(myDataSource)
 *   .withRedshiftTableName("my_schema.customer")
 *   .withRedshiftColumnNames(ImmutableList.of("customer_id", "address", "phone"))
 *   .withRedshiftIndexColumnNames(ImmutableList.of("customer_id")
 *   .withRedshiftIamRole("arn:aws:iam::123456789012:role/redshift-s3-read");
 * }
 *
 * This will provide loaders that load objects of type Customer into S3 using a provided bucket 'MyBucket' that is in
 * the 'us-east-1' region (it is an AWS requirement that the S3 bucket be located in the same region as the Redshift
 * cluster for this operation).
 *
 * A provided JDBC DataSource (myDataSource) will be used to connect to the Redshift instance and then load the all the
 * data into the my_schema.customer table. Redshift will assume a specific IAM role that was supplied to get the
 * access it needs to read the data from S3.
 *
 * The data that was serialized from the Customer.class object will be mapped to the destination columns customer_id,
 * address and phone. The customer_id column will be used to check if the each customer already exists in the table and
 * if so will be deleted and overwritten with the new record.
 *
 * Note that the properties in Customer.class to not have to be named identically to the Redshift table columns they
 * are being mapped to, but the order of the properties when serialized should match. This loader uses a Jackson CSV
 * serializer, so it is highly recommended to add a @JsonPropertyOrder annotation to your data class to ensure this
 * happens correctly. Eg:
 *
 * {@code
 * {@literal @JsonPropertyOrder({"customerId","address","phoneNumber"})}
 * Class Customer {
 *     public String customerId;
 *     public String address;
 *     public String phoneNumber;
 * }
 * }
 *
 * @param <T> The type of data being Loaded.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor
public class RedshiftBulkLoader<T> extends WrappedLoader<T> {
    private static final String DEFAULT_S3_PREFIX = "RedshiftCopyLoader";
    private static final RedshiftLoadStrategy DEFAULT_LOAD_STRATEGY = RedshiftLoadStrategy.MERGE_INTO_EXISTING_DATA;

    private final ParallelLoader<T> wrappedParallelLoader;

    /**
     * Create a supplier of RedshiftBulkLoader objects that is based on a specific class to load.
     *
     * @param classToLoad The class template for the objects being loaded.
     * @param <T> The type of object being loaded.
     * @return A newly constructed RedshiftBulkLoaderSupplier object.
     */
    public static <T> RedshiftBulkLoaderSupplier<T> supplierOf(Class<T> classToLoad) {
        return new RedshiftBulkLoaderSupplier<>(null, classToLoad, null, null,
                null, null, null, null, null, null,
                null, null, DEFAULT_LOAD_STRATEGY, null);
    }

    @Override
    protected Loader<T> getWrappedLoader() {
        return wrappedParallelLoader;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class RedshiftBulkLoaderSupplier<T> implements Supplier<Loader<T>> {
        private final String s3Bucket;
        private final Class<T> classToLoad;
        private final Integer bufferSizeInBytes;
        private final AmazonS3 amazonS3;
        private final String s3Prefix;
        private final String kmsArn;
        private final DataSource redshiftDataSource;
        private final String s3Region;
        private final String redshiftTableName;
        private final String redshiftIamRole;
        private final List<String> redshiftColumnNames;
        private final List<String> redshiftIndexColumnNames;
        private final RedshiftLoadStrategy redshiftLoadStrategy;
        private final RedshiftJdbcClient redshiftJdbcClient;

        /**
         * Required: Defines the name of the S3 bucket this loader should write its interim data to before copying it into
         * Redshift. Note that AWS currently requires the bucket to be hosted in the same region as the Redshift cluster
         * loading from it.
         *
         * @param s3Bucket The S3 bucket name.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withS3Bucket(String s3Bucket) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Optional: Defines the maximum size of each file written in S3. The defaulting behavior of this value is
         * implemented in the S3FastLoader class and is 128 MiB.
         *
         * @param bufferSizeInBytes Maximum size of each file written in S3 in bytes.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withBufferSizeInBytes(Integer bufferSizeInBytes) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Optional: Specifies an AmazonS3 client object to use for writing the data to S3 before loading it into
         * Redshift. If this property is not provided the loader will use the default AmazonS3 client builder.
         *
         * @param amazonS3 An AmazonS3 client object.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withAmazonS3(AmazonS3 amazonS3) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Optional: Specifies a key prefix that all files written to S3 as part of this load operation will have. Note that
         * the loader will add its own extension to this prefix that contains a random UUID among other things to ensure
         * that consecutive loads never interfere with each other. The default prefix is "RedshiftCopyLoader".
         *
         * @param s3Prefix S3 Key prefix
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withS3Prefix(String s3Prefix) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Optional: Specifies a KMS hosted key to use to encrypt the data being loaded onto and from S3 during this
         * load operation. Note that in order for this to work the AmazonS3 client and the IAM Role the Redshift Cluster
         * will be using needs access to use this key. This uses an implementation of Server Side Encryption (SSE).
         *
         * @param kmsArn ARN that identifies the KMS key to use.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withKmsArn(String kmsArn) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Optional: Specifies the load strategy to be used for bulk upload to redshift. See {@link RedshiftLoadStrategy}
         * for detailed information on different types of strategies. The default strategy is
         * {@link RedshiftLoadStrategy#MERGE_INTO_EXISTING_DATA}.
         *
         * @param redshiftLoadStrategy type of upload to redshift.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withLoadStrategy(RedshiftLoadStrategy redshiftLoadStrategy) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: Specifies the JDBC DataSource to use to interact with the Redshift cluster.
         *
         * @param redshiftDataSource A DataSource object that can connect to the Redshift cluster.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withRedshiftDataSource(DataSource redshiftDataSource) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: Specifies the S3 region which the S3 bucket and the Redshift cluster are both hosted on. Note that it
         * is an AWS requirement that they be hosted in the same region.
         *
         * @param s3Region The AWS region the S3 bucket and Redshift cluster are both hosted on.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withS3Region(String s3Region) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: The name of the Redshift table to load the data into. This can include a Redshift schema name if
         * appropriate.
         *
         * @param redshiftTableName Destination Redshift table name.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withRedshiftTableName(String redshiftTableName) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: The IAM role the Redshift Cluster will assume to read the data that was loaded into S3. Typically
         * this role will have explicit read-access to the S3 Bucket (and key prefix if applicable).
         *
         * @param redshiftIamRole An ARN that identifies the IAM role.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withRedshiftIamRole(String redshiftIamRole) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: The names of the columns in the destination Redshift table that the data being loaded should be
         * mapped into. This property is required because it is not assumed that the names of the columns in Redshift
         * will exactly match the names of the properties in the Java class, however the order the properties are written
         * must exactly match the order they are specifies in this list. The only way to guarantee that is to use the
         * {@literal @JsonPropertyOrder} annotation in your data class, which will be used when the objects are serialized
         * to CSV.
         *
         * @param redshiftColumnNames List of Redshift column names in the order they will be serialized.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withRedshiftColumnNames(List<String> redshiftColumnNames) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Required: The list of column names in the destination Redshift table that are used to index and uniquely
         * identify the data rows in the table. This is used by the loader to construct an SQL query that will find matching
         * rows that already exist in the destination Redshift table and delete them before overwriting them with the newly
         * loaded data. As Redshift does not enforce uniqueness, failure to do this correctly will result in duplicate
         * rows being found in the Redshift table after loading.
         *
         * @param redshiftIndexColumnNames List of Redshift column names that uniquely identify the data rows held within
         *                                 the destination table.
         * @return A copy of the current RedshiftBulkLoader with this property modified.
         */
        public RedshiftBulkLoaderSupplier<T> withRedshiftIndexColumnNames(List<String> redshiftIndexColumnNames) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        // Visible for testing.
        RedshiftBulkLoaderSupplier<T> withRedshiftJdbcClient(RedshiftJdbcClient redshiftJdbcClient) {
            return new RedshiftBulkLoaderSupplier<>(s3Bucket, classToLoad, bufferSizeInBytes, amazonS3, s3Prefix, kmsArn,
                    redshiftDataSource, s3Region, redshiftTableName, redshiftIamRole, redshiftColumnNames,
                    redshiftIndexColumnNames, redshiftLoadStrategy, redshiftJdbcClient);
        }

        /**
         * Invoke the provider to construct a loader object. If any of the required properties have not yet been set, this
         * will cause an exception to be thrown.
         *
         * @return A constructed and functional Loader object.
         * @throws RuntimeException if any of the required properties have not been set.
         */
        @Override
        public RedshiftBulkLoader<T> get() {
            checkRequiredProperty(s3Bucket, "s3Bucket");
            checkRequiredProperty(classToLoad, "classToLoad");
            checkRequiredProperty(redshiftDataSource, "redshiftDataSource");
            checkRequiredProperty(s3Region, "s3Region");
            checkRequiredProperty(redshiftTableName, "redshiftTableName");
            checkRequiredProperty(redshiftIamRole, "redshiftIamRole");
            checkRequiredProperty(redshiftColumnNames, "redshiftColumnNames");
            checkRequiredProperty(redshiftIndexColumnNames, "redshiftIndexColumnNames");

            Supplier<StringSerializer<T>> stringSerializerSupplier = () -> CsvStringSerializer.of(classToLoad)
                    .withColumnSeparator('|');

            final String finalS3Prefix = (s3Prefix == null ? DEFAULT_S3_PREFIX : s3Prefix) + "/" + UUID.randomUUID().toString();

            S3FastLoader.S3FastLoaderSupplier<T> loaderSupplier = S3FastLoader.supplierOf(s3Bucket, stringSerializerSupplier)
                    .withS3PartFileKeyGenerator((thread, partNum) ->
                            String.format("%s/%02d/part-%05d.csv", finalS3Prefix, thread, partNum));

            if (amazonS3 != null) {
                loaderSupplier = loaderSupplier.withClient(amazonS3);
            }

            if (bufferSizeInBytes != null) {
                loaderSupplier = loaderSupplier.withMaxPartFileSizeInBytes(bufferSizeInBytes);
            }

            if (kmsArn != null) {
                loaderSupplier = loaderSupplier.withSSEKmsArn(kmsArn);
            }

            RedshiftJdbcClient redshiftJdbcClient = this.redshiftJdbcClient == null ? new RedshiftJdbcClient(redshiftDataSource) : this.redshiftJdbcClient;

            return new RedshiftBulkLoader<>(ParallelLoader.of(loaderSupplier)
                    .withOnCloseCallback((dataWasLoaded, parentMetrics) -> {
                        try {
                            if (dataWasLoaded) {
                                switch (redshiftLoadStrategy) {
                                    case MERGE_INTO_EXISTING_DATA:
                                        redshiftJdbcClient.copyAndMerge(redshiftColumnNames, redshiftIndexColumnNames,
                                                                        redshiftTableName, s3Bucket, finalS3Prefix, s3Region, redshiftIamRole,
                                                                        parentMetrics);
                                        break;
                                    case CLOBBER_EXISTING_DATA:
                                        redshiftJdbcClient.deleteAndCopy(redshiftColumnNames, redshiftTableName, s3Bucket,
                                                                         finalS3Prefix, s3Region, redshiftIamRole, parentMetrics);
                                        break;
                                }
                            } else if (redshiftLoadStrategy.equals(RedshiftLoadStrategy.CLOBBER_EXISTING_DATA)) {
                                redshiftJdbcClient.truncate(redshiftTableName);
                            }
                        } catch (RuntimeException e) {
                            // Any kind of failure writing the data to Redshift constitutes a complete job failure due
                            // to the batchy nature of this operation
                            throw new UnrecoverableStreamFailureException(e);
                        }
                    }));
        }

        private void checkRequiredProperty(Object property, String propertyName) {
            if (property == null) {
                throw new RuntimeException("Cannot instantiate a RedshiftBulkLoader without '" + propertyName +
                        "' being set.");
            }
        }
    }
}
