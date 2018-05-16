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

package com.amazon.pocketEtl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class EtlTestBase {
    protected EtlProfilingScope etlProfilingScope;
    protected EtlMetrics mockMetrics;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    protected static class TestDTO {
        private String value;
    }

    @Before
    public void initializeServiceLogEntry() {
        mockMetrics = mock(EtlMetrics.class);
        associateMockMetricsProviders(mockMetrics, mockMetrics);
        etlProfilingScope = new EtlProfilingScope(mockMetrics, "UnitTest");
    }

    private void associateMockMetricsProviders(EtlMetrics mockParentMetrics, EtlMetrics mockChildMetrics) {
        when(mockParentMetrics.createChildMetrics()).thenReturn(mockChildMetrics);
    }

    @After
    public void teardownServiceLogEntry() {
        etlProfilingScope.close();
    }

}
