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

package com.amazon.pocketEtl.core;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class EtlStreamObjectTest {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO1 {
        private String first;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO2 {
        private String second;
        private String first;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO3 {
        private int first;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO4 {
        private DateTime first;
        private Instant second;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO4a {
        private DateTime first;
        private Instant second;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO5 {
        TestDTO1 outer;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO5a {
        TestDTO1 outer;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO6 {
        TestDTO2 outer;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO7 {
        Map<String, String> outer;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class TestDTO7a {
        Map<String, String> outer;
    }

    @Test
    public void settingAndThenGettingTheSameDTOProducesTwoEqualObjects() {
        TestDTO1 expectedDTO = TestDTO1.builder()
                                       .first("first-string")
                                       .build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(expectedDTO);
        TestDTO1 actualDTO = etlStreamObject.get(TestDTO1.class);

        assertThat(actualDTO, equalTo(expectedDTO));
    }

    @Test
    public void valuesNotCurrentlyInViewAreTunneled() {
        TestDTO2 initialDto = TestDTO2.builder()
                                      .first("first-string")
                                      .second("second-string")
                                      .build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(initialDto);
        TestDTO1 newDTO = etlStreamObject.get(TestDTO1.class);
        newDTO.setFirst("updated-first-string");
        etlStreamObject = etlStreamObject.with(newDTO);

        TestDTO2 finalDTO = etlStreamObject.get(TestDTO2.class);

        assertThat(finalDTO.getFirst(), equalTo("updated-first-string"));
        assertThat(finalDTO.getSecond(), equalTo("second-string"));
    }

    @Test
    public void valueWrittenAsAnIntegerCanBeLaterReadAsAString() {
        TestDTO3 initialDTO = TestDTO3.builder()
                                      .first(123)
                                      .build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(initialDTO);

        TestDTO1 finalDTO = etlStreamObject.get(TestDTO1.class);

        assertThat(finalDTO.getFirst(), equalTo("123"));
    }

    @Test
    public void valueWrittenAsAStringCanBeLaterReadAsAnInteger() {
        TestDTO1 initialDTO = TestDTO1.builder()
                                      .first("123")
                                      .build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(initialDTO);

        TestDTO3 finalDTO = etlStreamObject.get(TestDTO3.class);

        assertThat(finalDTO.getFirst(), equalTo(123));
    }

    @Test
    public void canSetAndGetDateTimeAndInstants() {
        DateTime dateTime = DateTime.now();
        Instant instant = Instant.now();
        EtlStreamObject etlStreamObject =
            EtlStreamObject.of(TestDTO4.builder().first(dateTime).second(instant).build());

        TestDTO4a result = etlStreamObject.get(TestDTO4a.class);

        assertThat(result.getFirst().withZone(dateTime.getZone()), equalTo(dateTime));
        assertThat(result.getSecond(), equalTo(instant));
    }

    @Test
    public void canSerializeAndDeserializeDeepDataStructures() {
        TestDTO5 objectToSerialize = TestDTO5.builder().outer(TestDTO1.builder().first("test").build()).build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(objectToSerialize);
        TestDTO5a result = etlStreamObject.get(TestDTO5a.class);

        assertThat(result.getOuter(), equalTo(objectToSerialize.getOuter()));
    }

    @Test
    public void canTunnelDeepDataStructures() {
        TestDTO6 objectToSerialize =
            TestDTO6.builder().outer(TestDTO2.builder().first("test-1").second("test-2").build()).build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(objectToSerialize);
        TestDTO5 intermediateObject = etlStreamObject.get(TestDTO5.class);
        intermediateObject.getOuter().setFirst("test-3");
        etlStreamObject = etlStreamObject.with(intermediateObject);

        TestDTO6 expectedObject =
            TestDTO6.builder().outer(TestDTO2.builder().first("test-3").second("test-2").build()).build();
        TestDTO6 actualObject = etlStreamObject.get(TestDTO6.class);

        assertThat(actualObject, equalTo(expectedObject));
    }

    @Test
    public void handlesMapsKeyedByString() {
        TestDTO7 objectToSerialize = TestDTO7.builder().outer(ImmutableMap.of("key1", "value1")).build();

        EtlStreamObject etlStreamObject = EtlStreamObject.of(objectToSerialize);
        TestDTO7a intermediateObject = etlStreamObject.get(TestDTO7a.class);

        assertThat(intermediateObject.getOuter().get("key1"), equalTo("value1"));
        etlStreamObject = etlStreamObject.with(intermediateObject);

        TestDTO7 result = etlStreamObject.get(TestDTO7.class);
        assertThat(result, equalTo(objectToSerialize));
    }

    // The default implementation of LoggingStrategy requires EtlStreamObject.get not to throw when converting to
    // Object.class
    @Test
    public void getAsGenericObjectDoesNotThrowException() {
        TestDTO1 objectToSerialize = TestDTO1.builder().first("test").build();
        EtlStreamObject etlStreamObject = EtlStreamObject.of(objectToSerialize);

        etlStreamObject.get(Object.class);
    }
}
