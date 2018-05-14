/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.core.EtlStreamObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MetricsEmissionEtlConsumerTest extends EtlTestBase {
    private final static String STAGE_NAME = "testStage";
    private final static String JOB_NAME = "jobName";

    @Mock
    private EtlStreamObject mockEtlStreamObject;

    @Mock
    private EtlConsumer mockDownstreamEtlConsumer;

    @Mock
    private EtlProfilingScope mockEtlProfilingScope;

    private MetricsEmissionEtlConsumer metricsEmissionConsumer;


    @Before
    public void createMetricsEmissionConsumer() {
        metricsEmissionConsumer = new MetricsEmissionEtlConsumer(JOB_NAME + "." + STAGE_NAME, mockDownstreamEtlConsumer);
    }

    @Test
    public void consumeEmitsACounterForEveryRecordConsumed() {
        metricsEmissionConsumer.open(mockMetrics);
        metricsEmissionConsumer.consume(mockEtlStreamObject);
        metricsEmissionConsumer.consume(mockEtlStreamObject);
        metricsEmissionConsumer.consume(mockEtlStreamObject);

        verify(mockMetrics, times(3))
                .addCount(eq(JOB_NAME + "." + STAGE_NAME + ".recordsProcessed"), eq(1.0));
    }

    @Test
    public void consumePassesRecordToDownstreamConsumer() {
        metricsEmissionConsumer.open(mockMetrics);
        metricsEmissionConsumer.consume(mockEtlStreamObject);

        verify(mockDownstreamEtlConsumer, times(1)).consume(eq(mockEtlStreamObject));
    }

    @Test
    public void openCallsOpenOnDownstreamConsumer() {
        metricsEmissionConsumer.open(mockEtlProfilingScope.getMetrics());

        verify(mockDownstreamEtlConsumer, times(1)).open(mockEtlProfilingScope.getMetrics());
    }

    @Test
    public void closeCallsCloseOnDownstreamConsumer() throws Exception {
        metricsEmissionConsumer.close();

        verify(mockDownstreamEtlConsumer, times(1)).close();
    }
}
