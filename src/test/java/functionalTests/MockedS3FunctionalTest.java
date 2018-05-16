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

package functionalTests;

import com.amazon.pocketEtl.EtlStream;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.extractor.IterableExtractor;
import com.amazon.pocketEtl.loader.CsvStringSerializer;
import com.amazon.pocketEtl.loader.S3FastLoader;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MockedS3FunctionalTest {
    private final List<String> outputStrings = new ArrayList<>();

    @Mock
    private AmazonS3 mockAmazonS3;

    @Before
    public void prepareMockS3() {
        when(mockAmazonS3.putObject(any(PutObjectRequest.class))).thenAnswer((Answer<PutObjectResult>) invocation -> {
            PutObjectRequest request = (PutObjectRequest)invocation.getArguments()[0];
            InputStream inputStream = request.getInputStream();
            outputStrings.add(IOUtils.toString(inputStream));
            return null;
        });
    }

    @Test
    public void testPartFilesWithHeadersOneRowPerFile() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"), new TestDTO("THREE"));
        Loader<TestDTO> loader = S3FastLoader.supplierOf("test-bucket", () -> CsvStringSerializer.of(TestDTO.class).withHeaderRow(true))
                .withMaxPartFileSizeInBytes(10)
                .withClient(mockAmazonS3)
                .get();

        EtlStream.extract(IterableExtractor.of(inputData))
                .load(TestDTO.class, loader)
                .run();

        List<String> expectedOutput = ImmutableList.of("value\nONE\n", "value\nTWO\n", "value\nTHREE\n");

        assertThat(outputStrings, equalTo(expectedOutput));
    }

    @Test
    public void testPartFilesWithHeadersMultiRowsPerFile() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"), new TestDTO("THREE"),
                new TestDTO("FOUR"));
        Loader<TestDTO> loader = S3FastLoader.supplierOf("test-bucket", () -> CsvStringSerializer.of(TestDTO.class).withHeaderRow(true))
                .withMaxPartFileSizeInBytes(17)
                .withClient(mockAmazonS3)
                .get();

        EtlStream.extract(IterableExtractor.of(inputData))
                .load(TestDTO.class, loader)
                .run();

        List<String> expectedOutput = ImmutableList.of("value\nONE\nTWO\n", "value\nTHREE\nFOUR\n");

        assertThat(outputStrings, equalTo(expectedOutput));
    }
}
