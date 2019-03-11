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

package com.amazon.pocketEtl.extractor;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqlExtractorTest extends EtlTestBase {
    private static final String EXTRACT_SQL = "SELECT 1 FROM atable WHERE one = #one AND two = #two AND three = #three AND four = #four";
    private static final String EXTRACT_SQL_WITH_ARRAY = "SELECT 1 FROM atable WHERE one IN (#testarray)";
    private static final String EXPECTED_SQL = "SELECT 1 FROM atable WHERE one = ? AND two = ? AND three = ? AND four = ?";
    private static final String EXPECTED_SQL_WITH_ARRAY = "SELECT 1 FROM atable WHERE one IN (?)";
    private static final String EXTRACT_SQL_WITH_WITH = "WITH x AS (SELECT 1 FROM atable WHERE one = #one) SELECT * FROM x";
    private static final String EXPECTED_SQL_WITH_WITH = "WITH x AS (SELECT 1 FROM atable WHERE one = ?) SELECT * FROM x";
    private static final String EXTRACT_SQL_WITH_OTHER_INFORMATION = "SELECT 1, 2, 3 FROM atable WHERE one = #one AND two = #two AND three = #three AND four = #four";

    private static final int INTEGER_COLUMN_VALUE = 1;
    private static final String COLUMN_NAME = "TestColumn";
    private static final String OTHER_INFORMATION_COLUMN_NAME_ONE = "TestColumnTwo";
    private static final String OTHER_INFORMATION_COLUMN_NAME_TWO = "TestColumnThree";
    private static final String COLUMN_VALUE1 = "ColumnValue1";
    private static final String COLUMN_VALUE2 = "ColumnValue2";
    private static final String COLUMN_VALUE3 = "ColumnValue3";
    private static final String OTHER_INFORMATION_COLUMN1_VALUE1 = "OtherInformationColumn1Value1";
    private static final String OTHER_INFORMATION_COLUMN1_VALUE2 = "OtherInformationColumn1Value2";
    private static final String OTHER_INFORMATION_COLUMN1_VALUE3 = "OtherInformationColumn1Value3";
    private static final String OTHER_INFORMATION_COLUMN2_VALUE1 = "OtherInformationColumn2Value1";
    private static final String OTHER_INFORMATION_COLUMN2_VALUE2 = "OtherInformationColumn2Value2";
    private static final String OTHER_INFORMATION_COLUMN2_VALUE3 = "OtherInformationColumn2Value3";

    private static final Map<String, String> OTHER_INFORMATION_MAP_ONE = ImmutableMap.of(
            OTHER_INFORMATION_COLUMN_NAME_ONE, OTHER_INFORMATION_COLUMN1_VALUE1,
            OTHER_INFORMATION_COLUMN_NAME_TWO, OTHER_INFORMATION_COLUMN2_VALUE1
    );
    private static final Map<String, String> OTHER_INFORMATION_MAP_TWO = ImmutableMap.of(
            OTHER_INFORMATION_COLUMN_NAME_ONE, OTHER_INFORMATION_COLUMN1_VALUE2,
            OTHER_INFORMATION_COLUMN_NAME_TWO, OTHER_INFORMATION_COLUMN2_VALUE2
    );
    private static final Map<String, String> OTHER_INFORMATION_MAP_THREE = ImmutableMap.of(
            OTHER_INFORMATION_COLUMN_NAME_ONE, OTHER_INFORMATION_COLUMN1_VALUE3,
            OTHER_INFORMATION_COLUMN_NAME_TWO, OTHER_INFORMATION_COLUMN2_VALUE3
    );

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BasicDTO {
        private String testColumn;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BasicOtherInformationDTO {
        private String testColumn;
        private Map<String, String> otherInformation = new LinkedHashMap<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DateTimeDTO {
        private DateTime testColumn;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IntegerDTO {
        private Integer testColumn;
    }

    private static final Map<String, ?> SQL_PARAMETERS = ImmutableMap.of(
            "one", 1,
            "two", 2L,
            "three", "Three",
            "four", 4.0
    );

    private static final String[] SQL_ARRAY = {"one", "two", "three"};
    private static final Map<String, ?> SQL_JAVA_STRING_LIST_PARAMETERS = ImmutableMap.of(
            "testarray", Arrays.asList(SQL_ARRAY)
    );

    @Mock
    private DataSource mockDatasource;

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private ResultSetMetaData mockResultSetMetaData;

    @Before
    public void constructSqlExtractor() throws SQLException {
        when(mockDatasource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.execute()).thenReturn(true);
        when(mockPreparedStatement.getResultSet()).thenReturn(mockResultSet);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(mockResultSetMetaData.getColumnLabel(eq(1))).thenReturn(COLUMN_NAME);
    }

    private SqlExtractor<BasicDTO> getBasicDTOSqlExtractor() {
        SqlExtractor<BasicDTO> sqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, BasicDTO.class)
                .withSqlParameters(SQL_PARAMETERS);
        sqlExtractor.open(mockMetrics);
        return sqlExtractor;
    }

    private SqlExtractor<IntegerDTO> getIntegerDTOSqlExtractor() {
        SqlExtractor<IntegerDTO> sqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, IntegerDTO.class)
                .withSqlParameters(SQL_PARAMETERS);
        sqlExtractor.open(mockMetrics);
        return sqlExtractor;
    }

    private SqlExtractor<BasicOtherInformationDTO> getBasicOtherInformationDTOSqlExtractor() {
        SqlExtractor<BasicOtherInformationDTO> sqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL_WITH_OTHER_INFORMATION, BasicOtherInformationDTO.class)
                .withSqlParameters(SQL_PARAMETERS)
                .withUnknownPropertyMapper((dto, entry) -> dto.getOtherInformation().put(entry.getKey(), entry.getValue()));

        sqlExtractor.open(mockMetrics);
        return sqlExtractor;
    }

    @Test
    public void nextReturnsNextValueWithOtherInformationFromResultSet() throws Exception {
        SqlExtractor<BasicOtherInformationDTO> otherInformationSqlExtractor = getBasicOtherInformationDTOSqlExtractor();
        when(mockResultSetMetaData.getColumnCount()).thenReturn(3);
        when(mockResultSetMetaData.getColumnLabel(eq(2))).thenReturn(OTHER_INFORMATION_COLUMN_NAME_ONE);
        when(mockResultSetMetaData.getColumnLabel(eq(3))).thenReturn(OTHER_INFORMATION_COLUMN_NAME_TWO);
        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenReturn(OTHER_INFORMATION_COLUMN1_VALUE1).thenReturn(OTHER_INFORMATION_COLUMN2_VALUE1)
                .thenReturn(COLUMN_VALUE2).thenReturn(OTHER_INFORMATION_COLUMN1_VALUE2).thenReturn(OTHER_INFORMATION_COLUMN2_VALUE2)
                .thenReturn(COLUMN_VALUE3).thenReturn(OTHER_INFORMATION_COLUMN1_VALUE3).thenReturn(OTHER_INFORMATION_COLUMN2_VALUE3);

        BasicOtherInformationDTO expectedResultOne = new BasicOtherInformationDTO(COLUMN_VALUE1, OTHER_INFORMATION_MAP_ONE);
        BasicOtherInformationDTO expectedResultTwo = new BasicOtherInformationDTO(COLUMN_VALUE2, OTHER_INFORMATION_MAP_TWO);
        BasicOtherInformationDTO expectedResultThree = new BasicOtherInformationDTO(COLUMN_VALUE3, OTHER_INFORMATION_MAP_THREE);
        BasicOtherInformationDTO actualResultOne = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);
        BasicOtherInformationDTO actualResultTwo = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);
        BasicOtherInformationDTO actualResultThree = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);

        assertThat(actualResultOne, equalTo(expectedResultOne));
        assertThat(actualResultTwo, equalTo(expectedResultTwo));
        assertThat(actualResultThree, equalTo(expectedResultThree));
        assertThat(otherInformationSqlExtractor.next(), equalTo(Optional.empty()));
    }

    @Test
    public void nextReturnsNextValueWithEmptyOtherInformationFromResultSet() throws Exception {
        SqlExtractor<BasicOtherInformationDTO> otherInformationSqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, BasicOtherInformationDTO.class)
                .withSqlParameters(SQL_PARAMETERS);
        otherInformationSqlExtractor.open(mockMetrics);

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenReturn(COLUMN_VALUE2).thenReturn(COLUMN_VALUE3);

        BasicOtherInformationDTO expectedResultOne = new BasicOtherInformationDTO(COLUMN_VALUE1, ImmutableMap.of());
        BasicOtherInformationDTO expectedResultTwo = new BasicOtherInformationDTO(COLUMN_VALUE2, ImmutableMap.of());
        BasicOtherInformationDTO expectedResultThree = new BasicOtherInformationDTO(COLUMN_VALUE3, ImmutableMap.of());
        BasicOtherInformationDTO actualResultOne = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);
        BasicOtherInformationDTO actualResultTwo = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);
        BasicOtherInformationDTO actualResultThree = otherInformationSqlExtractor.next().orElseThrow(RuntimeException::new);

        assertThat(actualResultOne, equalTo(expectedResultOne));
        assertThat(actualResultTwo, equalTo(expectedResultTwo));
        assertThat(actualResultThree, equalTo(expectedResultThree));
        assertThat(otherInformationSqlExtractor.next(), equalTo(Optional.empty()));
    }

    @Test
    public void nextReturnsNextValueFromResultSet() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();
        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenReturn(COLUMN_VALUE2).thenReturn(COLUMN_VALUE3);

        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(COLUMN_VALUE1)));
        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(COLUMN_VALUE2)));
        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(COLUMN_VALUE3)));
        assertThat(sqlExtractor.next(), equalTo(Optional.empty()));
    }

    @Test
    public void nextReturnsIntegerValueFromResultSet() throws Exception {
        SqlExtractor<IntegerDTO> sqlExtractor = getIntegerDTOSqlExtractor();
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getInt(anyInt())).thenReturn(INTEGER_COLUMN_VALUE);

        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new IntegerDTO(INTEGER_COLUMN_VALUE)));
    }

    @Test
    public void nextReturnsNullWhenValueIsNullFromResultSet() throws Exception {
        SqlExtractor<IntegerDTO> sqlExtractor = getIntegerDTOSqlExtractor();
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.wasNull()).thenReturn(true);
        when(mockResultSet.getInt(anyInt())).thenReturn(0);

        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new IntegerDTO(null)));
    }

    @Test
    public void nextReturnsDateTimeValueFromResultSet() throws Exception {
        DateTime now = DateTime.now();
        SqlExtractor<DateTimeDTO> dateTimeSqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, DateTimeDTO.class).withSqlParameters(SQL_PARAMETERS);
        dateTimeSqlExtractor.open(mockMetrics);

        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getTimestamp(anyInt())).thenReturn(new Timestamp(now.getMillis()));

        assertThat(dateTimeSqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new DateTimeDTO(now)));
    }

    @Test
    public void nextReturnsNullDateTimeValueFromResultSet() throws Exception {
        SqlExtractor<DateTimeDTO> dateTimeSqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, DateTimeDTO.class).withSqlParameters(SQL_PARAMETERS);
        dateTimeSqlExtractor.open(mockMetrics);

        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getTimestamp(anyInt())).thenReturn(null);

        assertThat(dateTimeSqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new DateTimeDTO(null)));
    }

    @Test
    public void nextCanHandleEmptyResultSet() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        when(mockResultSet.next()).thenReturn(false);

        assertThat(sqlExtractor.next(), equalTo(Optional.empty()));
    }

    @Test(expected = UnableToExecuteStatementException.class)
    public void openThrowsUnableToExecuteStatementExceptionIfQueryThrowsSqlException() throws Exception {
        when(mockPreparedStatement.execute()).thenThrow(new SQLException());
        getBasicDTOSqlExtractor();
    }

    @Test
    public void nextThrowsExceptionAndCanContinueWhenResultSetGetStringThrowsException() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        RuntimeException expectedException = new RuntimeException();

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenThrow(expectedException).thenReturn(COLUMN_VALUE3);

        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(COLUMN_VALUE1)));

        try {
            sqlExtractor.next();
            fail("Exception should have been thrown");
        } catch (RuntimeException e) {
            assertThat(e, sameInstance(expectedException));
        }

        assertThat(sqlExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(COLUMN_VALUE3)));
        assertThat(sqlExtractor.next(), equalTo(Optional.empty()));
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void nextThrowsUnrecoverableStreamFailureExceptionIfResultSetNextThrowsSqlException() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        when(mockResultSet.next()).thenThrow(new SQLException());

        sqlExtractor.next();
    }

    @Test(expected = RuntimeException.class)
    public void nextThrowsRuntimeExceptionIfResultSetGetThrowsSqlException() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getString(anyInt())).thenThrow(new SQLException());

        sqlExtractor.next();
    }

    @Test(expected = IllegalStateException.class)
    public void nextThrowsIllegalStateExceptionIfNotOpen() {
        SqlExtractor<BasicDTO> sqlExtractor =
                SqlExtractor.of(mockDatasource, EXTRACT_SQL, BasicDTO.class)
                        .withSqlParameters(SQL_PARAMETERS);

        sqlExtractor.next();
    }

    @Test
    public void closeClosesResources() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        sqlExtractor.next();
        sqlExtractor.close();

        verify(mockResultSet).close();
        verify(mockPreparedStatement).close();
        verify(mockConnection).close();
    }

    @Test(expected = IllegalStateException.class)
    public void nextAfterCloseThrowsIllegalStateException() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        sqlExtractor.close();
        sqlExtractor.next();
    }


    @Test
    public void executeQueryEmitsTimingMetricsForExecuteQuery() {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        sqlExtractor.next();

        verify(mockMetrics).addTime(eq("SqlExtractor.executeQuery"), anyDouble());
    }

    @Test
    public void closeEmitsTimingMetrics() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        sqlExtractor.close();

        verify(mockMetrics).addTime(eq("SqlExtractor.close"), anyDouble());
    }

    @Test
    public void runExtractCountsProcessingFailures() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenThrow(new RuntimeException()).thenThrow(new RuntimeException());

        sqlExtractor.next();

        try {
            sqlExtractor.next();
        } catch (RuntimeException ignored) {
        }

        try {
            sqlExtractor.next();
        } catch (RuntimeException ignored) {
        }

        verify(mockMetrics, times(2)).addCount(eq("SqlExtractor.extractionSuccess"), eq(0.0));
        verify(mockMetrics, times(2)).addCount(eq("SqlExtractor.extractionFailure"), eq(1.0));
    }

    @Test
    public void runExtractCountsProcessingSuccesses() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(anyInt())).thenReturn(COLUMN_VALUE1).thenReturn(COLUMN_VALUE2).thenThrow(new RuntimeException());

        sqlExtractor.next();
        sqlExtractor.next();

        try {
            sqlExtractor.next();
        } catch (RuntimeException ignored) {

        }

        verify(mockMetrics, times(2)).addCount(eq("SqlExtractor.extractionSuccess"), eq(1.0));
        verify(mockMetrics, times(2)).addCount(eq("SqlExtractor.extractionFailure"), eq(0.0));
    }

    @Test
    public void preparedStatementHasCorrectParametersAndSubstitution() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor = getBasicDTOSqlExtractor();

        sqlExtractor.next();

        verify(mockConnection).prepareStatement(eq(EXPECTED_SQL));
        verify(mockPreparedStatement).setInt(eq(1), eq((Integer) SQL_PARAMETERS.get("one")));
        verify(mockPreparedStatement).setLong(eq(2), eq((Long) SQL_PARAMETERS.get("two")));
        verify(mockPreparedStatement).setString(eq(3), eq((String) SQL_PARAMETERS.get("three")));
        verify(mockPreparedStatement).setDouble(eq(4), eq((Double) SQL_PARAMETERS.get("four")));
    }

    @Test
    public void preparedStatementCanHandleStringListAsParameter() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor;

        sqlExtractor = SqlExtractor.of(mockDatasource, EXTRACT_SQL_WITH_ARRAY, BasicDTO.class)
                .withSqlParameters(SQL_JAVA_STRING_LIST_PARAMETERS);
        Array mockArray = mock(Array.class);
        when(mockConnection.createArrayOf(anyString(), any())).thenReturn(mockArray);

        sqlExtractor.open(mockMetrics);
        sqlExtractor.next();

        verify(mockConnection).prepareStatement(eq(EXPECTED_SQL_WITH_ARRAY));
        verify(mockConnection).createArrayOf(eq("text"), eq(SQL_ARRAY));
        verify(mockPreparedStatement).setArray(eq(1), same(mockArray));
    }

    @Test
    public void preparedStatementCanHandleSubstitutionsInWithClause() throws Exception {
        SqlExtractor<BasicDTO> sqlExtractor;

        sqlExtractor = SqlExtractor.of(mockDatasource, EXTRACT_SQL_WITH_WITH, BasicDTO.class)
                .withSqlParameters(SQL_PARAMETERS);
        sqlExtractor.open(mockMetrics);

        sqlExtractor.next();

        verify(mockConnection).prepareStatement(eq(EXPECTED_SQL_WITH_WITH));
        verify(mockPreparedStatement).setInt(eq(1), eq((Integer) SQL_PARAMETERS.get("one")));
    }
}