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

import com.amazon.pocketEtl.EtlTestBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MetricsLoaderTest extends EtlTestBase {
    private static final String SAMPLE_METRIC_NAME = "sampleMetricName.";

    private static final double SAMPLE_DOUBLE = 1.1;
    private static final Integer SAMPLE_NULL_INTEGER = null;
    private static final int SAMPLE_NEGATIVE_INTEGER = -7;
    private static final int SAMPLE_POSITIVE_INTEGER = 7;
    private static final float SAMPLE_FLOAT = 0.0f;
    private static final long SAMPLE_LONG = 0L;
    private static final byte SAMPLE_BYTE = 11;
    private static final short SAMPLE_SHORT = 1;
    private static final String SAMPLE_STRING = "NA";
    private static final ImmutableMap<String, String> SAMPLE_MAP = ImmutableMap.of("keyOne", "valueOne");
    private static final ImmutableList<String> SAMPLE_LIST = ImmutableList.of("listItemOne", "listItemTwo");
    private static final DateTime SAMPLE_DATE_TIME = DateTime.now();

    private static final TestDTOWithIntegers TEST_DTO_WITH_INTEGERS = new TestDTOWithIntegers(SAMPLE_NEGATIVE_INTEGER, SAMPLE_POSITIVE_INTEGER);
    private static final TestDTOWithNullableInteger TEST_DTO_WITH_NULL_INTEGER = new TestDTOWithNullableInteger(SAMPLE_NULL_INTEGER);
    private static final TestDTO TEST_DTO_TWO = new TestDTO(SAMPLE_STRING, SAMPLE_FLOAT, SAMPLE_LONG, SAMPLE_BYTE, SAMPLE_SHORT,
            SAMPLE_DOUBLE, SAMPLE_MAP, SAMPLE_LIST, SAMPLE_DATE_TIME);

    private MetricsLoader<TestDTO> metricsLoader;
    private MetricsLoader<TestDTOWithIntegers> integerOnlyDtoMetricsLoader;
    private MetricsLoader<TestDTOWithNullableInteger> nullableIntegerMetricsLoader;

    @Data
    static class TestDTOWithIntegers {
        @JsonProperty("differentMetricName")
        private final int sampleInteger;

        private final int anotherInteger;
    }

    @Data
    static class TestDTOWithNullableInteger {
        @JsonProperty("anInteger")
        private final Integer anInteger;
    }

    @Data
    static class TestDTO {
        @JsonProperty("aString")
        private final String aString;
        @JsonProperty("aFloat")
        private final float aFloat;
        @JsonProperty("aLong")
        private final long aLong;
        @JsonProperty("aByte")
        private final byte aByte;
        @JsonProperty("aShort")
        private final short aShort;
        @JsonProperty("aDouble")
        private final double aDouble;
        @JsonProperty("aMap")
        private final Map aMap;
        @JsonProperty("aList")
        private final List aList;
        @JsonProperty("aDateTime")
        private final DateTime aDateTime;
    }

    @Before
    public void buildMetricsLoader() {
        metricsLoader = MetricsLoader.of(SAMPLE_METRIC_NAME);
        integerOnlyDtoMetricsLoader = MetricsLoader.of(SAMPLE_METRIC_NAME);
        nullableIntegerMetricsLoader = MetricsLoader.of(SAMPLE_METRIC_NAME);
    }

    @Test
    public void loadEmitsCounterForIntegerValue() throws Exception {
        integerOnlyDtoMetricsLoader.open(mockMetrics);
        integerOnlyDtoMetricsLoader.load(TEST_DTO_WITH_INTEGERS);
        integerOnlyDtoMetricsLoader.close();

        verify(mockMetrics).addCount(eq("sampleMetricName.differentMetricName"), eq(-7.0));
        verify(mockMetrics).addCount(eq("sampleMetricName.anotherInteger"), eq(7.0));
    }

    @Test
    public void loadDoesNotEmitCounterForNullInteger() throws Exception {
        nullableIntegerMetricsLoader.open(mockMetrics);
        nullableIntegerMetricsLoader.load(TEST_DTO_WITH_NULL_INTEGER);
        nullableIntegerMetricsLoader.close();

        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.anInteger"), anyInt());
    }

    @Test
    public void loadDoesNotEmitCounterForNoIntegerValue() throws Exception {
        metricsLoader.open(mockMetrics);
        metricsLoader.load(TEST_DTO_TWO);
        metricsLoader.close();

        verify(mockMetrics).addCount(eq("sampleMetricName.aByte"), eq(11.0));
        verify(mockMetrics).addCount(eq("sampleMetricName.aShort"), eq(1.0));

        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aString"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aFloat"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aLong"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aDouble"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aMap"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aList"), anyInt());
        verify(mockMetrics, times(0)).addCount(eq("sampleMetricName.aDateTime"), anyInt());
    }

    @Test
    public void loadDoesNotThrowExceptionOnNullMetrics() throws Exception {
        // Tests null metric does not throw exception
        integerOnlyDtoMetricsLoader.open(null);
        integerOnlyDtoMetricsLoader.load(TEST_DTO_WITH_INTEGERS);
        integerOnlyDtoMetricsLoader.close();
    }

    @Test
    public void loadUsesDefaultMetricNameToLogIfMetricNameIsNull() throws Exception {
        integerOnlyDtoMetricsLoader = MetricsLoader.of();

        integerOnlyDtoMetricsLoader.open(mockMetrics);
        integerOnlyDtoMetricsLoader.load(TEST_DTO_WITH_INTEGERS);
        integerOnlyDtoMetricsLoader.close();

        verify(mockMetrics).addCount(eq("MetricsLoader.differentMetricName"), eq(-7.0));
        verify(mockMetrics).addCount(eq("MetricsLoader.anotherInteger"), eq(7.0));
    }
}
