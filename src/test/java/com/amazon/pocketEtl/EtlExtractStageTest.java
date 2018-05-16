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

import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.amazon.pocketEtl.core.producer.EtlProducerFactory;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EtlExtractStageTest {
    private static String EXPECTED_DEFAULT_NAME = "EtlStream.Extract";

    @Mock
    private Extractor<?> mockExtractor;
    @Mock
    private EtlProducerFactory mockEtlProducerFactory;
    @Mock
    private EtlProducer mockEtlProducer;
    @Mock
    private EtlConsumer mockEtlConsumer;

    @Test
    public void staticConstructorWithSingleExtractor() {
        EtlExtractStage etlExtractStage = EtlExtractStage.of(mockExtractor);

        assertThat(etlExtractStage.getExtractors(), equalTo(Collections.singletonList(mockExtractor)));
        assertThat(etlExtractStage.getStageName(), equalTo(EXPECTED_DEFAULT_NAME));
    }

    @Test
    public void staticConstructorWithMultipleExtractors() {
        EtlExtractStage etlExtractStage = EtlExtractStage.of(ImmutableList.of(mockExtractor, mockExtractor));

        assertThat(etlExtractStage.getExtractors(), equalTo(ImmutableList.of(mockExtractor, mockExtractor)));
        assertThat(etlExtractStage.getStageName(), equalTo(EXPECTED_DEFAULT_NAME));
    }

    @Test
    public void withNameOverridesName() {
        String customName = "custom-name";
        EtlExtractStage etlExtractStage = EtlExtractStage.of(mockExtractor).withName(customName);

        assertThat(etlExtractStage.getStageName(), equalTo(customName));
    }

    @Test
    public void constructProducersForStageUsesFactory() {
        EtlExtractStage etlExtractStage = new EtlExtractStage(ImmutableList.of(mockExtractor, mockExtractor),
                EXPECTED_DEFAULT_NAME, mockEtlProducerFactory);
        when(mockEtlProducerFactory.newExtractorProducer(anyString(), any(), any())).thenReturn(mockEtlProducer);

        Collection<EtlProducer> result = etlExtractStage.constructProducersForStage(mockEtlConsumer);

        assertThat(result, contains(mockEtlProducer, mockEtlProducer));
        verify(mockEtlProducerFactory, times(2)).newExtractorProducer(EXPECTED_DEFAULT_NAME, mockExtractor, mockEtlConsumer);
    }
}