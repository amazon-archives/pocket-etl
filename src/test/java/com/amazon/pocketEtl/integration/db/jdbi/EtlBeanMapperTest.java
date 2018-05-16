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

package com.amazon.pocketEtl.integration.db.jdbi;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.amazon.pocketEtl.integration.db.jdbi.EtlBeanMapperTest.TestEnum.TEST_ENUM;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlBeanMapperTest {
    private static final byte TEST_BYTE = 12;
    private static final short TEST_SHORT = 13;
    private static final int TEST_INT = 14;
    private static final long TEST_LONG = 15L;
    private static final float TEST_FLOAT = 1.6F;
    private static final double TEST_DOUBLE = 1.7;
    private static final BigDecimal TEST_BIG_DECIMAL = new BigDecimal(1.8);
    private static final Timestamp TEST_TIMESTAMP = new Timestamp(19L);
    private static final DateTime TEST_DATETIME = new DateTime(TEST_TIMESTAMP.getTime());
    private static final Time TEST_TIME = new Time(20L);
    private static final Date TEST_DATE = new Date(21L);
    private static final String TEST_STRING = "22";

    @Mock private ResultSet mockResultSet;
    @Mock private ResultSetMetaData mockMetadata;
    @Mock private TestDTO mockObject;

    private EtlBeanMapper<TestDTO> etlBeanMapper = new EtlBeanMapper<>(TestDTO.class,
            (dto, entry) -> dto.getOtherInformation().put(entry.getKey(), entry.getValue()));

    public enum TestEnum {
        TEST_ENUM
    }

    @Data
    public static class TestDTO {
        private boolean testBoolean;
        private byte testByte;
        private short testShort;
        private int testInt;
        private long testLong;
        private float testFloat;
        private double testDouble;
        private BigDecimal testBigDecimal;
        private Timestamp testTimestamp;
        private DateTime testDateTime;
        private Time testTime;
        private Date testDate;
        private String testString;
        private TestEnum testEnum;
        private TestDTO testObject;
        private Map<String, String> otherInformation = new LinkedHashMap<>();
    }

    @Before
    public void initializeResultSet() throws Exception {
        when(mockResultSet.getMetaData()).thenReturn(mockMetadata);
        when(mockResultSet.getBoolean(anyInt())).thenReturn(true);
        when(mockResultSet.getByte(anyInt())).thenReturn(TEST_BYTE);
        when(mockResultSet.getShort(anyInt())).thenReturn(TEST_SHORT);
        when(mockResultSet.getInt(anyInt())).thenReturn(TEST_INT);
        when(mockResultSet.getLong(anyInt())).thenReturn(TEST_LONG);
        when(mockResultSet.getFloat(anyInt())).thenReturn(TEST_FLOAT);
        when(mockResultSet.getDouble(anyInt())).thenReturn(TEST_DOUBLE);
        when(mockResultSet.getBigDecimal(anyInt())).thenReturn(TEST_BIG_DECIMAL);
        when(mockResultSet.getTimestamp(anyInt())).thenReturn(TEST_TIMESTAMP);
        when(mockResultSet.getTime(anyInt())).thenReturn(TEST_TIME);
        when(mockResultSet.getDate(anyInt())).thenReturn(TEST_DATE);
        when(mockResultSet.getString(anyInt())).thenReturn(TEST_STRING);
        when(mockResultSet.getObject(anyInt())).thenReturn(mockObject);
    }

    @Test
    public void canMapBoolean() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testBoolean");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.isTestBoolean(), is(true));
        verify(mockResultSet).getBoolean(1);
    }

    @Test
    public void canMapByte() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testByte");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestByte(), equalTo(TEST_BYTE));
        verify(mockResultSet).getByte(1);
    }

    @Test
    public void canMapShort() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testShort");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestShort(), equalTo(TEST_SHORT));
        verify(mockResultSet).getShort(1);
    }

    @Test
    public void canMapInt() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testInt");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestInt(), equalTo(TEST_INT));
        verify(mockResultSet).getInt(1);
    }

    @Test
    public void canMapLong() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testLong");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestLong(), equalTo(TEST_LONG));
        verify(mockResultSet).getLong(1);
    }

    @Test
    public void canMapFloat() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testFloat");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestFloat(), equalTo(TEST_FLOAT));
        verify(mockResultSet).getFloat(1);
    }

    @Test
    public void canMapDouble() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testDouble");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestDouble(), equalTo(TEST_DOUBLE));
        verify(mockResultSet).getDouble(1);
    }

    @Test
    public void canMapBigDecimal() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testBigDecimal");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestBigDecimal(), equalTo(TEST_BIG_DECIMAL));
        verify(mockResultSet).getBigDecimal(1);
    }

    @Test
    public void canMapTimestamp() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testTimestamp");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestTimestamp(), equalTo(TEST_TIMESTAMP));
        verify(mockResultSet).getTimestamp(1);
    }

    @Test
    public void canMapDateTime() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testDateTime");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestDateTime(), equalTo(TEST_DATETIME));
        verify(mockResultSet).getTimestamp(1);
    }

    @Test
    public void canMapTime() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testTime");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestTime(), equalTo(TEST_TIME));
        verify(mockResultSet).getTime(1);
    }

    @Test
    public void canMapDate() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testDate");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestDate(), equalTo(TEST_DATE));
        verify(mockResultSet).getDate(1);
    }

    @Test
    public void canMapString() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testString");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestString(), equalTo(TEST_STRING));
        verify(mockResultSet).getString(1);
    }

    @Test
    public void canMapEnum() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testEnum");
        when(mockResultSet.getString(anyInt())).thenReturn(TEST_ENUM.name());

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestEnum(), equalTo(TEST_ENUM));
        verify(mockResultSet).getString(1);
    }

    @Test
    public void canMapObject() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testObject");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getTestObject(), equalTo(mockObject));
        verify(mockResultSet).getObject(1);
    }

    @Test
    public void stringOtherInformation() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testOther");
        when(mockMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getOtherInformation(), equalTo(ImmutableMap.of("testOther", TEST_STRING)));
    }

    @Test
    public void timestampOtherInformation() throws Exception {
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testOtherTimestamp");
        when(mockMetadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        String expectedTimestamp = TEST_DATETIME.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        assertThat(result.getOtherInformation(), equalTo(ImmutableMap.of("testOtherTimestamp", expectedTimestamp)));
    }

    @Test
    public void stringOtherInformationIgnoredWithoutSecondaryMapper() throws Exception {
        etlBeanMapper = new EtlBeanMapper<>(TestDTO.class, null);
        when(mockMetadata.getColumnCount()).thenReturn(1);
        when(mockMetadata.getColumnLabel(1)).thenReturn("testOther");

        TestDTO result = etlBeanMapper.map(0, mockResultSet, null);

        assertThat(result.getOtherInformation(), equalTo(ImmutableMap.of()));
    }
}
