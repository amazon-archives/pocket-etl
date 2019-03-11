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

package com.amazon.pocketEtl.integration;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.exception.DependencyException;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedshiftJdbcClientTest extends EtlTestBase {
    private static final List<String> FILE_COLUMN_NAMES = ImmutableList.of("filecol1", "filecol2", "filecol3");
    private static final List<String> KEY_COLUMN_NAMES = ImmutableList.of("keycol1", "keycol2", "keycol3");
    private static final String DESTINATION_TABLE = "dest_table";
    private static final String S3_BUCKET = "s3bucket";
    private static final String S3_PREFIX = "a/prefix";
    private static final String S3_REGION = "s3Region";
    private static final String IAM_ROLE = "iamRole";
    private static final long DATE_TIME_FIXED_MILLIS = 1234L;

    private static final String COPY_AND_MERGE_SQL_1 =
            "drop table if exists stage_1234";

    private static final String COPY_AND_MERGE_SQL_2 =
            "create temp table stage_1234 (like dest_table including defaults)";

    private static final String COPY_AND_MERGE_SQL_3 =
            "copy stage_1234(filecol1,filecol2,filecol3) " +
                    "from 's3://s3bucket/a/prefix/' " +
                    "iam_role 'iamRole' " +
                    "region 's3Region' " +
                    "removequotes";

    private static final String COPY_AND_MERGE_SQL_4 =
            "delete from dest_table " +
                    "using stage_1234 " +
                    "where dest_table.keycol1 = stage_1234.keycol1 " +
                    "and dest_table.keycol2 = stage_1234.keycol2 " +
                    "and dest_table.keycol3 = stage_1234.keycol3";

    private static final String COPY_AND_MERGE_SQL_5 =
            "insert into dest_table select * from stage_1234";

    private static final String COPY_AND_MERGE_SQL_6 =
            "drop table stage_1234";

    private static final String DELETE_AND_COPY_SQL_1 =
            "delete from dest_table";

    private static final String DELETE_AND_COPY_SQL_2 =
            "copy dest_table(filecol1,filecol2,filecol3) " +
                    "from 's3://s3bucket/a/prefix/' " +
                    "iam_role 'iamRole' " +
                    "region 's3Region' " +
                    "removequotes";

    private static final String TRUNCATE_SQL =
            "truncate dest_table";

    @Mock
    private DataSource mockRedshiftDataSource;

    @Mock
    private Connection mockRedshiftConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    private RedshiftJdbcClient redshiftJdbcClient;

    @Before
    public void initializeRedshiftJdbcClient() {
        redshiftJdbcClient = new RedshiftJdbcClient(mockRedshiftDataSource);
    }

    @Before
    public void freezeTime() {
        DateTimeUtils.setCurrentMillisFixed(DATE_TIME_FIXED_MILLIS);
    }

    @After
    public void unfreezeTime() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Before
    public void setupRedshiftDataSource() throws Exception {
        when(mockRedshiftDataSource.getConnection()).thenReturn(mockRedshiftConnection);
        when(mockRedshiftConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockRedshiftConnection.isClosed()).thenReturn(false);
    }

    @Test
    public void copyAndMergeGeneratesAndExecutesSqlCorrectly() throws Exception {
        redshiftJdbcClient.copyAndMerge(FILE_COLUMN_NAMES, KEY_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);

        InOrder inOrder = inOrder(mockPreparedStatement, mockRedshiftConnection);
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_1));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_2));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_3));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).setAutoCommit(eq(false));
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_4));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_5));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).commit();
        inOrder.verify(mockRedshiftConnection).setAutoCommit(eq(true));
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_6));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).close();
    }

    @Test
    public void copyAndMergeRollsbackOnSQLException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you"));

        try {
            redshiftJdbcClient.copyAndMerge(FILE_COLUMN_NAMES, KEY_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                                            S3_REGION, IAM_ROLE, mockMetrics);
        } catch (RuntimeException ignored) {
        }

        verify(mockRedshiftConnection).rollback();
    }

    @Test
    public void copyAndMergeStillClosesAfterRollBackException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you")).thenReturn(true);
        doThrow(new SQLException("Test Exception")).when(mockRedshiftConnection).rollback();

        try {
            redshiftJdbcClient.copyAndMerge(FILE_COLUMN_NAMES, KEY_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                                            S3_REGION, IAM_ROLE, mockMetrics);
            fail("Excepted exception to be thrown");
        } catch (RuntimeException ignored) {
        }

        verify(mockRedshiftConnection).close();
    }

    @Test
    public void copyAndMergeAttemptsToDropTableIfConnectionIsStillOpen() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you"))
                .thenReturn(true);

        try {
            redshiftJdbcClient.copyAndMerge(FILE_COLUMN_NAMES, KEY_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);
            fail("Excepted exception to be thrown");
        } catch (RuntimeException ignored) {
        }

        InOrder inOrder = inOrder(mockPreparedStatement, mockRedshiftConnection);
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_1));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(COPY_AND_MERGE_SQL_1));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).close();
    }

    @Test(expected = DependencyException.class)
    public void copyAndMergeThrowsResourceNotFoundOnNullConnection() throws Exception {
        when(mockRedshiftDataSource.getConnection()).thenReturn(null);

        redshiftJdbcClient.copyAndMerge(FILE_COLUMN_NAMES, KEY_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);
    }

    @Test
    public void deleteAndCopyGeneratesAndExecutesSqlCorrectly() throws Exception {
        redshiftJdbcClient.deleteAndCopy(FILE_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);

        InOrder inOrder = inOrder(mockPreparedStatement, mockRedshiftConnection);
        inOrder.verify(mockRedshiftConnection).setAutoCommit(eq(false));
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(DELETE_AND_COPY_SQL_1));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(DELETE_AND_COPY_SQL_2));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).commit();
        inOrder.verify(mockRedshiftConnection).setAutoCommit(eq(true));
        inOrder.verify(mockRedshiftConnection).close();
    }

    @Test
    public void deleteAndCopyRollsbackOnSQLException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you"));

        try {
            redshiftJdbcClient.deleteAndCopy(FILE_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);
            fail("Excepted exception to be thrown");
        } catch (RuntimeException ignored) {
        }

        verify(mockRedshiftConnection).rollback();
    }

    @Test
    public void deleteAndCopyStillClosesAfterRollBackException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you")).thenReturn(true);
        doThrow(new SQLException("Test Exception")).when(mockRedshiftConnection).rollback();

        try {
            redshiftJdbcClient.deleteAndCopy(FILE_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);
            fail("Excepted exception to be thrown");
        } catch (RuntimeException ignored) {
        }

        verify(mockRedshiftConnection).close();
    }

    @Test(expected = DependencyException.class)
    public void deleteAndCopyThrowsResourceNotFoundOnNullConnection() throws Exception {
        when(mockRedshiftDataSource.getConnection()).thenReturn(null);

        redshiftJdbcClient.deleteAndCopy(FILE_COLUMN_NAMES, DESTINATION_TABLE, S3_BUCKET, S3_PREFIX,
                S3_REGION, IAM_ROLE, mockMetrics);
    }

    @Test
    public void truncateGeneratesAndExecutesSqlCorrectly() throws Exception {
        redshiftJdbcClient.truncate(DESTINATION_TABLE);

        InOrder inOrder = inOrder(mockPreparedStatement, mockRedshiftConnection);
        inOrder.verify(mockRedshiftConnection).prepareStatement(eq(TRUNCATE_SQL));
        inOrder.verify(mockPreparedStatement).execute();
        inOrder.verify(mockRedshiftConnection).close();
    }

    @Test
    public void truncateClosesConnectionAfterSQLException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException("Redshift hates you"));

        try {
            redshiftJdbcClient.truncate(DESTINATION_TABLE);
            fail("Excepted exception to be thrown");
        } catch (RuntimeException ignored) {
        }

        verify(mockRedshiftConnection).close();
    }

    @Test(expected = DependencyException.class)
    public void truncateThrowsResourceNotFoundOnNullConnection() throws Exception {
        when(mockRedshiftDataSource.getConnection()).thenReturn(null);

        redshiftJdbcClient.truncate(DESTINATION_TABLE);
    }
}
