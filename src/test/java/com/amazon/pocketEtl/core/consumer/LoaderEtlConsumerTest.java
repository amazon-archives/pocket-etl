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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.core.EtlStreamObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LoaderEtlConsumerTest extends EtlTestBase {
    private static final String TEST_NAME = "TestName";

    @Mock
    private Loader<TestDTO> mockLoader;

    @Mock
    private EtlConsumer mockErrorEtlConsumer;

    @Mock
    private EtlStreamObject mockEtlStreamObject;

    @Mock
    private TestDTO mockTestDTO;

    private LoaderEtlConsumer<TestDTO> loaderConsumer;

    @Before
    public void constructWorker() {
        when(mockEtlStreamObject.get(any())).thenReturn(mockTestDTO);
        loaderConsumer = new LoaderEtlConsumer<>(TEST_NAME, mockLoader, TestDTO.class, mockErrorEtlConsumer);
    }

    @Test
    public void consumeLoadsASingleObject() {
        loaderConsumer.open(mockMetrics);
        loaderConsumer.consume(mockEtlStreamObject);

        verify(mockLoader, times(1)).load(eq(mockTestDTO));
    }

    @Test
    public void closeClosesLoader() throws Exception {
        loaderConsumer.open(mockMetrics);
        loaderConsumer.close();

        verify(mockLoader).close();
    }

    @Test
    public void closeClosesErrorConsumer() throws Exception {
        loaderConsumer.open(mockMetrics);
        loaderConsumer.close();

        verify(mockErrorEtlConsumer).close();
    }

    @Test
    public void closeClosesErrorConsumerEvenAfterARuntimeException() throws Exception {
        doThrow(new RuntimeException("Test exception")).when(mockLoader).close();
        loaderConsumer.open(mockMetrics);
        loaderConsumer.close();

        verify(mockErrorEtlConsumer).close();
    }

    @Test
    public void openOpensLoader() throws Exception {
        loaderConsumer.open(etlProfilingScope.getMetrics());

        verify(mockLoader).open(eq(etlProfilingScope.getMetrics()));
    }

    @Test
    public void openOpensErrorConsumer() throws Exception {
        loaderConsumer.open(etlProfilingScope.getMetrics());

        verify(mockErrorEtlConsumer).open(eq(etlProfilingScope.getMetrics()));
    }

    @Test
    public void consumePassesToTheErrorConsumerOnRuntimeException() {
        doThrow(new RuntimeException("test")).when(mockLoader).load(any(TestDTO.class));

        loaderConsumer.open(mockMetrics);
        loaderConsumer.consume(mockEtlStreamObject);

        verify(mockErrorEtlConsumer).consume(mockEtlStreamObject);
    }
}
