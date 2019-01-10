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

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Optional;
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
            "\"java8DateTime\": \"1970-01-01T00:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]," +
            "\"optionalString\" : \"sampleOptionalString\"" +
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
            "\"java8DateTime\": \"1970-01-01T00:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]" +
            "}, {" +
            "\"aInt\": 20," +
            "\"aString\": \"sampleStringTwo\"," +
            "\"aDouble\": 4.125," +
            "\"aBoolean\": true," +
            "\"aDateTime\": \"2017-07-14T12:00:00Z\"," +
            "\"java8DateTime\": \"1970-01-01T00:00:00Z\"," +
            "\"aList\": [\"listItemOne\", \"listItemTwo\", \"listItemThree\"]" +
            "}]";

    private static final String SAMPLE_VALID_JSON_WITH_NULL_OPTIONAL_FIELD_VALUE =
            "{" +
            "\"optionalString\" : null" +
            "}";

    private static final TestDTO SAMPLE_TEST_DTO = new TestDTO(
            10,
            "sampleString",
            4.125,
            true,
            DateTime.parse("2017-08-14T12:00:00Z"),
            Instant.ofEpochMilli(0),
            ImmutableList.of("listItemOne", "listItemTwo", "listItemThree"),
            Optional.of("sampleOptionalString"));

    private Function<String, TestDTO> mapper = JSONStringMapper.of(TestDTO.class);

    @Test
    public void mapReturnsMappedDTOGivenAJsonStringAndDtoClass() {
        TestDTO result = mapper.apply(SAMPLE_VALID_JSON);

        assertThat(result, is(SAMPLE_TEST_DTO));
    }

    @Test
    public void mapReturnsNullWhenJsonStringIsNull() {
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

    @Test
    public void mapReturnsMappedDtoWithOptionalFieldBeingEmptyWhenJsonStringHasOptionalFieldWithNullValue() {
        TestDTO result = mapper.apply(SAMPLE_VALID_JSON_WITH_NULL_OPTIONAL_FIELD_VALUE);

        assertThat(result.getOptionalString(), is(Optional.empty()));
    }

    @Test
    public void mapReturnsMappedDtoWithOptionalFieldBeingNullWhenJsonStringHasMissingOptionalField() {
        TestDTO result = mapper.apply(SAMPLE_JSON_WITH_LESS_FIELDS_THAN_IN_DTO);

        assertThat(result.getOptionalString(), is(nullValue()));
    }
}