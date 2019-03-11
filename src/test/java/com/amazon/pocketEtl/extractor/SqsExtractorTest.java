/*
 *   Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqsExtractorTest extends EtlTestBase {
    private static final String SAMPLE_TEST_STRING_ONE = "testStringOne";
    private static final String SAMPLE_TEST_STRING_TWO = "testStringTwo";
    private static final String SAMPLE_TEST_STRING_THREE = "testStringThree";

    private static final String SAMPLE_MESSAGE_BODY_ONE = "{\"testString\": \"" + SAMPLE_TEST_STRING_ONE + "\"}";
    private static final String SAMPLE_MESSAGE_BODY_TWO = "{\"testString\": \"" + SAMPLE_TEST_STRING_TWO + "\"}";
    private static final String SAMPLE_MESSAGE_BODY_THREE = "{\"testString\": \"" + SAMPLE_TEST_STRING_THREE + "\"}";
    private static final String SAMPLE_INVALID_MESSAGE_BODY = "{\"invalid}";

    private static final String SAMPLE_QUEUE_URL = "sampleQueueUrl";
    private static final String SAMPLE_EXCEPTION = "sampleException";
    private static final String SAMPLE_RECEIPT_HANDLE = "sampleReceiptHandleOne";
    private static final String SAMPLE_RECEIPT_HANDLE_TWO = "sampleReceiptHandleTwo";
    private static final String SAMPLE_RECEIPT_HANDLE_THREE = "sampleReceiptHandleThree";
    private static final String SAMPLE_RECEIPT_HANDLE_FOUR = "sampleReceiptHandleFour";

    @Mock
    private AmazonSQS mockAmazonSQS;

    @Mock
    private ReceiveMessageResult mockReceiveMessageResult;

    @Captor
    private ArgumentCaptor<ReceiveMessageRequest> receiveMessageRequestArgumentCaptor;

    private Message sampleMessageOne, sampleMessageTwo, sampleMessageThree, sampleMessageFour;
    private SqsExtractor<BasicDTO> sqsExtractor;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BasicDTO {
        private String testString;
    }

    @Before
    public void initializeMessages() {
        sampleMessageOne = new Message();
        sampleMessageOne.setBody(SAMPLE_MESSAGE_BODY_ONE);
        sampleMessageOne.setReceiptHandle(SAMPLE_RECEIPT_HANDLE);
        sampleMessageTwo = new Message();
        sampleMessageTwo.setBody(SAMPLE_MESSAGE_BODY_TWO);
        sampleMessageTwo.setReceiptHandle(SAMPLE_RECEIPT_HANDLE_TWO);
        sampleMessageThree = new Message();
        sampleMessageThree.setBody(SAMPLE_MESSAGE_BODY_THREE);
        sampleMessageThree.setReceiptHandle(SAMPLE_RECEIPT_HANDLE_THREE);
        sampleMessageFour = new Message();
        sampleMessageFour.setBody(SAMPLE_INVALID_MESSAGE_BODY);
        sampleMessageFour.setReceiptHandle(SAMPLE_RECEIPT_HANDLE_FOUR);
    }

    @Before
    public void constructSqsExtractor(){
        when(mockAmazonSQS.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(mockReceiveMessageResult);

        sqsExtractor = SqsExtractor.of(SAMPLE_QUEUE_URL, JSONStringMapper.of(BasicDTO.class)).withClient(mockAmazonSQS).get();
        sqsExtractor.open(mockMetrics);
    }

    @Test
    public void nextReturnsNextValueFromMessageListIgnoringDuplicates() {
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne, sampleMessageTwo))
                .thenReturn(ImmutableList.of(sampleMessageOne, sampleMessageThree))
                .thenReturn(ImmutableList.of());

        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_ONE)));
        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_TWO)));
        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_THREE)));
        assertThat(sqsExtractor.next(), equalTo(Optional.empty()));
    }

    @Test
    public void whenBatchSizeLimitIsNotSetMaximumSqsMessagesRequested() {
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne, sampleMessageTwo))
                .thenReturn(ImmutableList.of());

        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_ONE)));
        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_TWO)));
        assertThat(sqsExtractor.next(), equalTo(Optional.empty()));

        verify(mockAmazonSQS, times(2)).receiveMessage(receiveMessageRequestArgumentCaptor.capture());
        assertThat(receiveMessageRequestArgumentCaptor.getValue().getMaxNumberOfMessages(), equalTo(10));
    }

    @Test
    public void batchSizeLimitIsCorrectlyApplied() {
        sqsExtractor = SqsExtractor.of(SAMPLE_QUEUE_URL, JSONStringMapper.of(BasicDTO.class))
                .withClient(mockAmazonSQS)
                .withBatchSizeLimit(2)
                .get();

        sqsExtractor.open(mockMetrics);

        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne, sampleMessageTwo))
                .thenReturn(ImmutableList.of(sampleMessageThree))
                .thenReturn(ImmutableList.of());

        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_ONE)));
        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_TWO)));
        assertThat(sqsExtractor.next(), equalTo(Optional.empty()));

        verify(mockAmazonSQS, times(1)).receiveMessage(receiveMessageRequestArgumentCaptor.capture());
        assertThat(receiveMessageRequestArgumentCaptor.getValue().getMaxNumberOfMessages(), equalTo(2));
    }

    @Test
    public void nextReturnsEmptyValueWhenSqsMessageQueueIsEmpty() {
        when(mockReceiveMessageResult.getMessages()).thenReturn(ImmutableList.of());

        assertThat(sqsExtractor.next(), equalTo(Optional.empty()));
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void nextThrowsUnrecoverableStreamFailureExceptionIfQueueNameIsInvalidOrNull() {
        when(mockAmazonSQS.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(new AmazonClientException(SAMPLE_EXCEPTION));

        sqsExtractor.next();
    }

    @Test
    public void nextRetriesThreeTimesBeforeThrowingUnrecoverableStreamFailureExceptionInCaseOfServiceException() {
        when(mockAmazonSQS.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(new AmazonServiceException(SAMPLE_EXCEPTION));

        try {
            sqsExtractor.next();
        } catch (UnrecoverableStreamFailureException ignored) {}

        verify(mockAmazonSQS, times(3)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    public void nextRetriesTwoTimesBeforeSuccess() {
        when(mockAmazonSQS.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenThrow(new AmazonServiceException(SAMPLE_EXCEPTION))
                .thenReturn(mockReceiveMessageResult);
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne))
                .thenReturn(ImmutableList.of());

        assertThat(sqsExtractor.next().orElseThrow(RuntimeException::new), equalTo(new BasicDTO(SAMPLE_TEST_STRING_ONE)));
        verify(mockAmazonSQS, times(3)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    public void nextEmitsSuccessCounters() {
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne))
                .thenReturn(ImmutableList.of());
        sqsExtractor.next();

        verify(mockMetrics).addCount(eq("SqsExtractor.extractionSuccess"), eq(1.0));
    }

    @Test
    public void nextEmitsFailureCounter() {
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageFour))
                .thenReturn(ImmutableList.of());

        try{
            sqsExtractor.next();
        } catch (RuntimeException e){
            // swallow exception so we can validate metrics
        }

        verify(mockMetrics).addCount(eq("SqsExtractor.extractionFailure"), eq(1.0));
    }

    @Test
    public void closeEmitsTimingMetrics() throws Exception{
        sqsExtractor.close();

        verify(mockMetrics).addTime(eq("SqsExtractor.close"), anyDouble());
    }

    @Test(expected = IllegalStateException.class)
    public void nextAfterCloseThrowsIllegalStateException() throws Exception {
        sqsExtractor.close();
        sqsExtractor.next();
    }

    @Test
    public void closeDeletesExtractedSqsMessages() throws Exception {
        when(mockReceiveMessageResult.getMessages())
                .thenReturn(ImmutableList.of(sampleMessageOne, sampleMessageTwo, sampleMessageThree))
                .thenReturn(ImmutableList.of());

        sqsExtractor.next();
        sqsExtractor.next();
        sqsExtractor.close();

        verify(mockAmazonSQS, times(2)).deleteMessage(anyString(), anyString());
    }
}
