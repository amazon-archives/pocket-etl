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

package com.amazon.pocketEtl.extractor;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CsvInputStreamMapperTest {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonPropertyOrder({"testData", "testNumber"})
    public static class TestDTO {
        public String testData;
        public int testNumber;
    }

    private CsvInputStreamMapper<TestDTO> csvInputStreamMapper = CsvInputStreamMapper.of(TestDTO.class);

    @Test
    public void repeatedCallsToNextReturnsObjectsPopulatedWithDataExtractedFromInputStream() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",123\n\"WORKERID2\",234");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        TestDTO testDTO1 = iterator.next();
        TestDTO testDTO2 = iterator.next();

        assertThat(testDTO1, is(equalTo(new TestDTO("WORKERID1", 123))));
        assertThat(testDTO2, is(equalTo(new TestDTO("WORKERID2", 234))));
    }

    @Test
    public void supportsPipeSeparator() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\"|123\n\"WORKERID2\"|234");
        Iterator<TestDTO> iterator = csvInputStreamMapper.withColumnSeparator('|').apply(inputStream);

        TestDTO testDTO1 = iterator.next();
        TestDTO testDTO2 = iterator.next();

        assertThat(testDTO1, is(equalTo(new TestDTO("WORKERID1", 123))));
        assertThat(testDTO2, is(equalTo(new TestDTO("WORKERID2", 234))));
    }

    @Test
    public void supportsTabSeparator() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\"\t123\n\"WORKERID2\"\t234");
        Iterator<TestDTO> iterator = csvInputStreamMapper.withColumnSeparator('\t').apply(inputStream);

        TestDTO testDTO1 = iterator.next();
        TestDTO testDTO2 = iterator.next();

        assertThat(testDTO1, is(equalTo(new TestDTO("WORKERID1", 123))));
        assertThat(testDTO2, is(equalTo(new TestDTO("WORKERID2", 234))));
    }

    @Test
    public void nextSupportsCRLF() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",123\r\n\"WORKERID2\",234");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        TestDTO testDTO1 = iterator.next();
        TestDTO testDTO2 = iterator.next();

        assertThat(testDTO1, is(equalTo(new TestDTO("WORKERID1", 123))));
        assertThat(testDTO2, is(equalTo(new TestDTO("WORKERID2", 234))));
    }

    @Test
    public void nextTreatsMissingFieldsAsZeroValueOrEmptyString() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",\r\n,234");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        TestDTO testDTO1 = iterator.next();
        TestDTO testDTO2 = iterator.next();

        assertThat(testDTO1, is(equalTo(new TestDTO("WORKERID1", 0))));
        assertThat(testDTO2, is(equalTo(new TestDTO("", 234))));
    }

    @Test
    public void hasNextReturnsFalseIfInputStreamIsEmpty() {
        InputStream inputStream = createInputStreamFromString("");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test
    public void hasNextReturnsFalseAtEndOfInputStream() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",123");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        assertThat(iterator.hasNext(), equalTo(true));
        iterator.next();
        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test
    public void nextReturnsEmptyObjectAtEndOfInputStreamWithNewline() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",123\n");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        assertThat(iterator.hasNext(), equalTo(true));
        iterator.next();
        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test(expected = RuntimeJsonMappingException.class)
    public void nextThrowsRuntimeJsonMappingExceptionIfInputStreamContainsRowWithExtraColumn() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",123,\"WORKERID1\"");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        iterator.next();
    }

    @Test(expected = RuntimeJsonMappingException.class)
    public void nextThrowsRuntimeJsonMappingExceptionIfInputStreamContainsRowWithMissingColumn() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\"");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        iterator.next();
    }

    @Test(expected = RuntimeJsonMappingException.class)
    public void nextThrowsRuntimeJsonMappingExceptionIfInputStreamContainsWrongDataType() {
        InputStream inputStream = createInputStreamFromString("\"WORKERID1\",\"BadNumber\"");
        Iterator<TestDTO> iterator = csvInputStreamMapper.apply(inputStream);

        iterator.next();
    }

    @Test(expected = RuntimeException.class)
    public void nextThrowsRuntimeExceptionIfInputStreamThrowsIOException() throws Exception {
        InputStream mockInputStream = mock(InputStream.class);
        when(mockInputStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("Test Exception"));
        csvInputStreamMapper.apply(mockInputStream);
    }

    private InputStream createInputStreamFromString(String inputString) {
        return new ByteArrayInputStream(inputString.getBytes(Charset.forName("UTF-8")));
    }
}
