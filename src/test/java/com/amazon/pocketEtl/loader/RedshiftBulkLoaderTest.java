/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Loader;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private AmazonS3 mockAmazonS3;

    @Before
    public void initializeMockDataSource() throws SQLException {
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    }

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
    public void newRedshiftViaS3LoaderCreatesLoaderWhichCallsCopyAndMergeOnCloseIfDataWasLoaded() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier()
                .withAmazonS3(mockAmazonS3)
                .get();

        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.load(OBJECT_TO_WRITE);
        redshiftLoader.close();

        verify(mockDataSource).getConnection();
    }

    @Test
    public void newRedshiftViaS3LoaderProducesLoaderWhichDoesNotCallCopyAndMergeOnCloseIfDataWasNotLoaded() throws Exception {
        Loader<TestDTO> redshiftLoader = getMinimalLoaderSupplier().get();

        redshiftLoader.open(etlProfilingScope.getMetrics());
        redshiftLoader.close();

        verifyNoMoreInteractions(mockDataSource);
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
