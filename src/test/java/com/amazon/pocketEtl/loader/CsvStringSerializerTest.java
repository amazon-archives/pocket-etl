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

package com.amazon.pocketEtl.loader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(MockitoJUnitRunner.class)
public class CsvStringSerializerTest {
    private static final String SAMPLE_STRING = "Foo";
    private static final int SAMPLE_COUNT = 7;
    private static final double SAMPLE_AMOUNT = 1.5;
    private static final ImmutableList<String> SAMPLE_LIST = ImmutableList.of("first", "second");
    private static final DateTime SAMPLE_DATE_TIME = DateTime.parse("2017-09-29T00:00:00.000Z");

    private static final TestDTO TEST_DTO = new TestDTO(SAMPLE_STRING, SAMPLE_COUNT, SAMPLE_AMOUNT, SAMPLE_LIST, SAMPLE_DATE_TIME);
    private static final TestDTO2 TEST_DTO2 = new TestDTO2(SAMPLE_STRING, SAMPLE_COUNT);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Data
    @JsonPropertyOrder({"name", "count", "amount", "list", "dateTime"})
    static class TestDTO {
        private final String name;
        private final int count;
        private final double amount;
        private final List<String> list;
        private final DateTime dateTime;
    }

    @Data
    @JsonPropertyOrder({"Test two", "Test one"})
    static class TestDTO2 {
        @JsonProperty("Test one")
        private final String testOne;
        @JsonProperty("Test two")
        private final int testTwo;
    }

    @Data
    public class DTOWithBadDataType {
        final String name;
        final private TestDTO testDTO;
    }

    @Test
    public void serializerConvertsDTOToStringWithDelimiterPSV() throws Exception {
        CsvStringSerializer<TestDTO> serializerForPSV = CsvStringSerializer.of(TestDTO.class).withColumnSeparator('|');

        // Need to change timezone to UTC for assertion as jackson serializes datetime to UTC time zone.
        String expectedUTCDateTimeString = SAMPLE_DATE_TIME.toDateTime(DateTimeZone.UTC).toString();
        String expectedResult = "Foo|7|1.5|first;second|" + expectedUTCDateTimeString + "\n";

        final String result = serializerForPSV.apply(TEST_DTO);

        assertThat(result, equalTo(expectedResult));
    }

    @Test
    public void serializerConvertsDTOToStringWithDelimiterCSV() throws Exception {
        CsvStringSerializer<TestDTO> serializerForCSV = CsvStringSerializer.of(TestDTO.class).withColumnSeparator(',');

        // Need to change timezone to UTC for assertion as jackson serializes datetime to UTC time zone.
        String expectedUTCDateTimeString = SAMPLE_DATE_TIME.toDateTime(DateTimeZone.UTC).toString();
        String expectedResult = "Foo,7,1.5,first;second," + expectedUTCDateTimeString + "\n";

        final String result = serializerForCSV.apply(TEST_DTO);

        assertThat(result, equalTo(expectedResult));
    }

    @Test
    public void serializerWritesHeaderRow() throws Exception {
        CsvStringSerializer<TestDTO> serializerForCSV = CsvStringSerializer.of(TestDTO.class)
                .withColumnSeparator(',')
                .withHeaderRow(true);

        // Need to change timezone to UTC for assertion as jackson serializes datetime to UTC time zone.
        String expectedUTCDateTimeString = SAMPLE_DATE_TIME.toDateTime(DateTimeZone.UTC).toString();
        String expectedResult = "name,count,amount,list,dateTime\n" +
                "Foo,7,1.5,first;second," + expectedUTCDateTimeString + "\n";

        final String result = serializerForCSV.apply(TEST_DTO);

        assertThat(result, equalTo(expectedResult));
    }

    @Test
    public void serializerWritesHeaderRowWithCustomNames() throws Exception {
        CsvStringSerializer<TestDTO2> serializerForCSV = CsvStringSerializer.of(TestDTO2.class)
                .withColumnSeparator('|')
                .withHeaderRow(true);

        String expectedResult = "Test two|Test one\n" +
                "7|Foo\n";
        String expectedResult2 = "7|Foo\n";

        final String result = serializerForCSV.apply(TEST_DTO2);
        final String result2 = serializerForCSV.apply(TEST_DTO2);

        assertThat(result, equalTo(expectedResult));
        assertThat(result2, equalTo(expectedResult2));
    }

    @Test(expected = RuntimeException.class)
    public void applyThrowsRunTimeExceptionForUnsupportedDataTypeInDTO() throws Exception {
        CsvStringSerializer<DTOWithBadDataType> serializerForCSV = CsvStringSerializer.of(DTOWithBadDataType.class)
                .withColumnSeparator(',');

        DTOWithBadDataType dtoWithBadDataType = new DTOWithBadDataType(SAMPLE_STRING, TEST_DTO);
        serializerForCSV.apply(dtoWithBadDataType);
    }
}