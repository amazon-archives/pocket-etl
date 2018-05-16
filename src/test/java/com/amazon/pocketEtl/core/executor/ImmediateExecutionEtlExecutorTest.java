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

package com.amazon.pocketEtl.core.executor;

import com.amazon.pocketEtl.EtlTestBase;
import org.junit.Test;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class ImmediateExecutionEtlExecutorTest extends EtlTestBase {
    private final ImmediateExecutionEtlExecutor etlExecutor = new ImmediateExecutionEtlExecutor();

    @Test
    public void executorCanDoRealWork() throws Exception {
        AtomicInteger workCounter = new AtomicInteger(0);

        IntStream.range(0, 100).forEach(i -> etlExecutor.submit(workCounter::incrementAndGet, etlProfilingScope.getMetrics()));
        etlExecutor.shutdown();

        assertThat(workCounter.get(), equalTo(100));
    }

    @Test
    public void verifyIsShutdownIsFalseByDefault() {
        boolean isShutdown = etlExecutor.isShutdown();

        assertThat(isShutdown, is(false));
    }

    @Test
    public void verifyIsShutdownIsTrueAfterShutdown() throws Exception {
        etlExecutor.shutdown();
        boolean isShutdown = etlExecutor.isShutdown();

        assertThat(isShutdown, is(true));
    }

    @Test(expected = RejectedExecutionException.class)
    public void submitAfterShutdownThrowsIllegalStateException() throws Exception {
        etlExecutor.shutdown();
        etlExecutor.submit(() -> {}, etlProfilingScope.getMetrics());
    }

    @Test
    public void submitSwallowsException() {
        etlExecutor.submit(() -> {
            throw new RuntimeException("Test Exception");
            }, etlProfilingScope.getMetrics());
    }

    @Test
    public void submitWrapsExecutionInServiceLogEntryScope() {
        etlExecutor.submit(() -> {}, etlProfilingScope.getMetrics());

        verify(mockMetrics).addTime(eq("SingleThreadedEtlExecutor.submit"), anyDouble());
    }
}
