/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EtlProfilingScopeTest {
    private static final String SCOPE_NAME = "scope-name";
    private static final String COUNTER_NAME = "counter-name";

    @Mock
    private EtlMetrics mockMetrics;

    @Mock
    private EtlMetrics mockChildMetrics;

    private EtlProfilingScope etlProfilingScope;

    @Before
    public void stubMetricsAndCreateScope() {
        when(mockMetrics.createChildMetrics()).thenReturn(mockChildMetrics);
        etlProfilingScope = new EtlProfilingScope(mockMetrics, SCOPE_NAME);
        verify(mockChildMetrics, never()).addTime(anyString(), anyDouble());
    }

    @Test
    public void close_emitsTime() {
        etlProfilingScope.close();

        verify(mockChildMetrics).addTime(eq(SCOPE_NAME), anyDouble());
    }

    @Test
    public void close_afterClose_doesNotEmitsTime() {
        etlProfilingScope.close();
        Mockito.reset(mockChildMetrics);

        etlProfilingScope.close();

        verify(mockChildMetrics, never()).addTime(anyString(), anyDouble());
    }

    @Test
    public void addCounter_emitsCount() {
        etlProfilingScope.addCounter(COUNTER_NAME, 123);

        verify(mockChildMetrics).addCount(COUNTER_NAME, 123.0);
    }

    @Test
    public void addCounter_doesNotEmitCountAfterClose() {
        etlProfilingScope.close();
        etlProfilingScope.addCounter(COUNTER_NAME, 123);

        verify(mockChildMetrics, never()).addCount(anyString(), anyDouble());
    }
}