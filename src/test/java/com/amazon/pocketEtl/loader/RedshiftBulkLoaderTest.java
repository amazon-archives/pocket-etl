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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;
import com.amazon.pocketEtl.integration.RedshiftJdbcClient;

import com.amazonaws.services.s3.AmazonS3;

@RunWith(MockitoJUnitRunner.class)
public class RedshiftBulkLoaderTest extends EtlTestBase {

    private static final String S3_BUCKET = "aS3Bucket";
    private static final String S3_PREFIX = "aGoodPrefix";
    private static final String S3_REGION = "s3Region";
    private static final TestDTO OBJECT_TO_WRITE = new TestDTO("somethingToWrite");
    private static final String DESTINATION_TABLE_NAME = "destinationTable";
    private static final List<String> EXTRACT_COLUMN_NAMES = ImmutableList.of("ecol1", "ecol2", "ecol3");
    private static final List<String> KEY_COLUMN_NAMES = ImmutableList.of("kcol1", "kcol2", "kcol3");
    private static final String IAM_ROLE = "iamRole";

    @Mock
    private DataSource mockDataSource;

    @Mock
    private AmazonS3 mockAmazonS3;

    @Mock
    private RedshiftJdbcClient mockRedshiftJdbcClient;

    @Test
    public void canCreateALoaderWithMinimumProperties() {
        getMinimalLoaderSupplier().get();
    }

    @Test
    public void canCreateALoaderWithAllProperties() {
        getMinimalLoaderSupplier()
                .withS3Prefix(S3_PREFIX)
                .withKmsArn("kms-arn")
                .withBufferSizeInBytes(1000000)
                .withAmazonS3(mockAmazonS3)
                .get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutS3BucketThrowsRTE() {
        getMinimalLoaderSupplier().withS3Bucket(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutS3RegionThrowsRTE() {
        getMinimalLoaderSupplier().withS3Region(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutRedshiftDataSourceThrowsRTE() {
        getMinimalLoaderSupplier().withRedshiftDataSource(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutRedshiftTableNameThrowsRTE() {
        getMinimalLoaderSupplier().withRedshiftTableName(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutRedshiftColumnNamesThrowsRTE() {
        getMinimalLoaderSupplier().withRedshiftColumnNames(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutRedshiftIndexColumnNamesThrowsRTE() {
        getMinimalLoaderSupplier().withRedshiftIndexColumnNames(null).get();
    }

    @Test(expected = RuntimeException.class)
    public void creatingALoaderWithoutRedshiftIamRoleThrowsRTE() {
        getMinimalLoaderSupplier().withRedshiftIamRole(null).get();
    }

    @Test
    public void newRedshiftViaS3LoaderCreatesLoaderWhichCallsCopyAndMergeOnCloseIfDataWasLoadedWithDefaultLoadStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withAmazonS3(mockAmazonS3)
                .withS3Prefix(S3_PREFIX)
                .withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .get();


        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.load(OBJECT_TO_WRITE);
        redshiftLoader.close();

        verify(mockRedshiftJdbcClient).copyAndMerge(eq(EXTRACT_COLUMN_NAMES), eq(KEY_COLUMN_NAMES), eq(DESTINATION_TABLE_NAME),
                eq(S3_BUCKET), anyString(), eq(S3_REGION), eq(IAM_ROLE), eq(mockMetrics));
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void loaderThrowsUnrecoverableStreamFailureExceptionOnRedshiftProblem() throws Exception {
        doThrow(new RuntimeException("Something went wrong")).when(mockRedshiftJdbcClient)
                                                             .copyAndMerge(anyList(), anyList(), anyString(),
                                                                           anyString(), anyString(), anyString(),
                                                                           anyString(), any(EtlMetrics.class));

        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
            .withAmazonS3(mockAmazonS3)
            .withS3Prefix(S3_PREFIX)
            .withRedshiftJdbcClient(mockRedshiftJdbcClient)
            .get();


        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.load(OBJECT_TO_WRITE);
        redshiftLoader.close();
    }


    @Test
    public void newRedshiftViaS3LoaderCreatesLoaderWhichCallsCopyAndMergeOnCloseIfDataWasLoadedWithMergeIntoExistingDataStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withAmazonS3(mockAmazonS3)
                .withS3Prefix(S3_PREFIX)
                .withLoadStrategy(RedshiftLoadStrategy.MERGE_INTO_EXISTING_DATA)
                .withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .get();


        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.load(OBJECT_TO_WRITE);
        redshiftLoader.close();

        verify(mockRedshiftJdbcClient).copyAndMerge(eq(EXTRACT_COLUMN_NAMES), eq(KEY_COLUMN_NAMES), eq(DESTINATION_TABLE_NAME),
                eq(S3_BUCKET), anyString(), eq(S3_REGION), eq(IAM_ROLE), eq(mockMetrics));
    }

    @Test
    public void newRedshiftViaS3LoaderCreatesLoaderWhichCallsDeleteAndCopyOnCloseIfDataWasLoadedWithClobberExistingDataStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withAmazonS3(mockAmazonS3)
                .withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .withLoadStrategy(RedshiftLoadStrategy.CLOBBER_EXISTING_DATA)
                .get();

        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.load(OBJECT_TO_WRITE);
        redshiftLoader.close();

        verify(mockRedshiftJdbcClient).deleteAndCopy(eq(EXTRACT_COLUMN_NAMES), eq(DESTINATION_TABLE_NAME),
                eq(S3_BUCKET), anyString(), eq(S3_REGION), eq(IAM_ROLE), eq(mockMetrics));
    }

    @Test
    public void newRedshiftViaS3LoaderCreatesLoaderWhichCallsTruncateOnCloseIfDataWasNotLoadedWithClobberExistingDataStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withAmazonS3(mockAmazonS3)
                .withLoadStrategy(RedshiftLoadStrategy.CLOBBER_EXISTING_DATA)
                .withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .get();


        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.close();

        verify(mockRedshiftJdbcClient).truncate(DESTINATION_TABLE_NAME);
    }

    @Test
    public void newRedshiftViaS3LoaderProducesLoaderWhichDoesNotCallTruncateOnCloseIfDataWasNotLoadedWithDefaultStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier().
                withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .get();

        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.close();

        verifyNoMoreInteractions(mockRedshiftJdbcClient);
    }

    @Test
    public void newRedshiftViaS3LoaderProducesLoaderWhichDoesNotCallTruncateOnCloseIfDataWasNotLoadedWithMergeIntoExistingDataStrategy() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withLoadStrategy(RedshiftLoadStrategy.MERGE_INTO_EXISTING_DATA)
                .withRedshiftJdbcClient(mockRedshiftJdbcClient)
                .get();

        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.close();

        verifyNoMoreInteractions(mockRedshiftJdbcClient);
    }

    private RedshiftBulkLoader.RedshiftBulkLoaderSupplier<TestDTO> getMinimalLoaderSupplier() {
        return RedshiftBulkLoader.supplierOf(TestDTO.class)
                .withS3Bucket(S3_BUCKET)
                .withS3Region(S3_REGION)
                .withRedshiftDataSource(mockDataSource)
                .withRedshiftTableName(DESTINATION_TABLE_NAME)
                .withRedshiftColumnNames(EXTRACT_COLUMN_NAMES)
                .withRedshiftIndexColumnNames(KEY_COLUMN_NAMES)
                .withRedshiftIamRole(IAM_ROLE);
    }
}
