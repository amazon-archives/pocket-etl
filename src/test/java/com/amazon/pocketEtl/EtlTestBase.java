/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
