/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlCombineStageTest {
    private final static String EXPECTED_DEFAULT_STAGE_NAME = "EtlStream.Combine";

    @Mock
    private EtlStream mockEtlStream1;
    @Mock
    private EtlStream mockEtlStream2;
    @Mock
    private EtlStageChain mockEtlStageChain1;
    @Mock
    private EtlStageChain mockEtlStageChain2;
    @Mock
    private EtlConsumer mockEtlConsumer;
    @Mock
    private EtlProducer mockEtlProducer1;
    @Mock
    private EtlProducer mockEtlProducer2;
    @Mock
    private EtlProducer mockEtlProducer3;

    private EtlCombineStage etlCombineStage;

    @Before
    public void constructEtlCombineStage() {
        when(mockEtlStream1.getStageChain()).thenReturn(mockEtlStageChain1);
        when(mockEtlStream2.getStageChain()).thenReturn(mockEtlStageChain2);
        when(mockEtlStageChain1.constructComponentProducers(any())).thenReturn(ImmutableList.of(mockEtlProducer1));
        when(mockEtlStageChain2.constructComponentProducers(any())).thenReturn(ImmutableList.of(mockEtlProducer2, mockEtlProducer3));
        etlCombineStage = EtlCombineStage.of(ImmutableList.of(mockEtlStream1, mockEtlStream2));

    }

    @Test
    public void staticConstructorSetsPropertiesAsExpected() {
        assertThat(etlCombineStage.getStageChains(), equalTo(ImmutableList.of(mockEtlStageChain1, mockEtlStageChain2)));
        assertThat(etlCombineStage.getStageName(), equalTo(EXPECTED_DEFAULT_STAGE_NAME));
    }

    @Test
    public void combineStreamsCorrectlyFlattensComponentProducers() {
        Collection<EtlProducer> result = etlCombineStage.constructProducersForStage(mockEtlConsumer);

        assertThat(result, contains(mockEtlProducer1, mockEtlProducer2, mockEtlProducer3));
    }
}