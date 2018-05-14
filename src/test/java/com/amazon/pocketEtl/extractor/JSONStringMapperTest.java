/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.extractor;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@RunWith(MockitoJUnitRunner.class)
public class JSONStringMapperTest {
    private static final String SAMPLE_VALID_JSON =
            "{" +
            "\"aInt\": 10," +
            "\"aString\": \"sampleString\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-08-14T12:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]" +
            "}";

    private static final String SAMPLE_INVALID_JSON = "{\"name\"}";

    private static final String SAMPLE_JSON_WITH_A_FIELD_MISSING_FROM_DTO =
            "{" +
            "\"aInt\": 10," +
            "\"aString\": \"sampleString\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-08-14T12:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]," +
            "\"extraString\": \"extraString\"" +
            "}";

    private static final String SAMPLE_JSON_WITH_LESS_FIELDS_THAN_IN_DTO =
            "{" +
            "\"aInt\": 10," +
            "\"aString\": \"sampleString\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-08-14T12:00:00Z\"" +
            "}";

    private static final String SAMPLE_JSON_HAVING_MORE_THAN_ONE_OBJECT =
            "[{" +
            "\"aInt\": 10," +
            "\"aString\": \"sampleString\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-08-14T12:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]" +
            "}, {" +
            "\"aInt\": 20," +
            "\"aString\": \"sampleStringTwo\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-07-14T12:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]" +
            "}]";

    private static final TestDTO SAMPLE_TEST_DTO = new TestDTO(
            10,
            "sampleString",
            4.125,
            true,
            DateTime.parse("2017-08-14T12:00:00Z"),
            ImmutableList.of("listItemOne", "listItemTwo", "listItemThree"));

    private Function<String, TestDTO> mapper = JSONStringMapper.of(TestDTO.class);

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TestDTO {
        private int aInt;
        private String aString;
        private double aDouble;
        private boolean aBoolean;
        private DateTime aDateTime;
        private List<String> aList;
    }

    @Test
    public void mapReturnsMappedDTOGivenAJsonStringAndDtoClass() throws Exception {
        TestDTO result = mapper.apply(SAMPLE_VALID_JSON);
        assertThat(result, is(SAMPLE_TEST_DTO));
    }

    @Test
    public void mapReturnsNullWhenJsonStringIsNull() throws Exception {
        TestDTO result = mapper.apply(null);

        assertThat(result, nullValue());
    }

    @Test(expected = RuntimeException.class)
    public void mapThrowsRunTimeExceptionIfJsonIsNotvalid() {
        mapper.apply(SAMPLE_INVALID_JSON);
    }

    @Test(expected = RuntimeException.class)
    public void factoryThrowsRunTimeExceptionIfDtoClassIsNull() {
        JSONStringMapper.of(null);
    }

    @Test(expected = RuntimeException.class)
    public void mapThrowsRunTimeExceptionIfJsonHasFieldNotPresentInDto() {
        mapper.apply(SAMPLE_JSON_WITH_A_FIELD_MISSING_FROM_DTO);
    }

    @Test
    public void mapReturnsMappedDtoWhenJsonStringHasLessFieldsThanDto() {
        TestDTO expected = new TestDTO();
        expected.setAInt(10);
        expected.setAString("sampleString");
        expected.setADouble(4.125);
        expected.setABoolean(true);
        expected.setADateTime(DateTime.parse("2017-08-14T12:00:00Z"));

        TestDTO result = mapper.apply(SAMPLE_JSON_WITH_LESS_FIELDS_THAN_IN_DTO);

        assertThat(result, is(expected));
    }

    @Test(expected = RuntimeException.class)
    public void mapThrowsRunTimeExceptionIfJsonStringContainsMoreThanOneObject() {
        mapper.apply(SAMPLE_JSON_HAVING_MORE_THAN_ONE_OBJECT);
    }
}