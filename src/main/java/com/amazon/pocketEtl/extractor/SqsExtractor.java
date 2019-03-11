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

import static org.apache.logging.log4j.LogManager.getLogger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * An implementation of Extractor that uses SQS as backing store. A function that can translate string messages from the
 * queue into data objects must be provided.
 * It deletes all extracted messages from SQS on close.
 *
 * Usage example:
 *
 * EtlStream.extract(SqsExtractor.of(mySqsUrl, JSONStringMapper.of(MySimpleData.class).withClient(mySqsClient));
 *
 * @param <T> Type of object that is extracted from the SQS.
 */
@SuppressWarnings("WeakerAccess")
public class SqsExtractor<T> implements Extractor<T> {
    private static final Logger logger = getLogger(SqsExtractor.class);
    private static final int SQS_MAXIMUM_MESSAGES_PER_REQUEST = 10;
    private static final int SQS_GET_MESSAGES_MAX_RETRIES = 3;
    private static final int SQS_LONG_POLLING_PERIOD_IN_SECONDS = 20;
    private static final Supplier<AmazonSQS> DEFAULT_SQS_CLIENT_BUILDER = () -> AmazonSQSClient.builder().build();

    private Iterator<Message> messageIterator;
    private List<String> extractedMessagesReceiptHandle;

    private final AmazonSQS amazonSqs;
    private final Function<String, T> stringMapper;
    private final String queueUrl;
    private final int batchSizeLimit;

    private boolean isClosed = false;
    private EtlMetrics parentMetrics = null;

    /**
     * Creates a Supplier object that will construct SqlExtractor objects on demand based on supplied parameters.
     * Out of the box the extractor will use the default Amazon SQS client. This can be modified as required.
     * This supplier object can be passed directly to an EtlStream. Example:
     *
     * EtlStream.extract(SqlExtractor.of(mySQSUrl, JSONStringMapper.of(MyDTO.class));
     *
     * @param queueUrl SQS queue URL to dequeue from.
     * @param stringMapper Function that can map SQS message body strings to DTO objects.
     * @param <T> Type of object being extracted.
     * @return A builder that be extended with additional configuration or passed as an extractor to EtlStream.
     */
    public static <T> SqsExtractorProvider<T> of(String queueUrl, Function<String, T> stringMapper) {
        return new SqsExtractorProvider<>(null, queueUrl, stringMapper, null);
    }

    /**
     * Provides an SqlExtractor. Can be passed to an EtlStream extractor stage when ready. Out of the box the
     * extractor will use the default Amazon SQS client. This can be modified as required.
     * @param <T> Type of object being extracted.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class SqsExtractorProvider<T> implements Supplier<Extractor> {
        private final AmazonSQS sqsClient;
        private final String queueUrl;
        private final Function<String, T> stringMapper;
        private final Integer batchSizeLimit;

        /**
         * Modify the AWS SQS Client object to be used with the extractor.
         * @param amazonSQS An initialized AWS SQS Client.
         * @return A new SqlExtractorProvider with modified configuration.
         */
        public SqsExtractorProvider<T> withClient(AmazonSQS amazonSQS) {
            return new SqsExtractorProvider<>(amazonSQS, queueUrl, stringMapper, batchSizeLimit);
        }

        /**
         * Set a limit to how many messages will be read off the SQS queue. This is necessary because the ETL job will
         * keep all the messages in flight and only delete them when the whole batch has been successfully processed.
         * If the length of time the ETL job takes to complete its entire batch exceeds the visibility timeout setting
         * for messages read from the SQS queue, then those messages will be deemed failed by SQS itself which causes
         * unnecessary re-driving and errors. Setting this value will protect you against spirals of death resulting
         * from excessively large queue backlogs
         * @param batchSizeLimit Maximum number of records that will be read from the queue in a single Etl Job. A null
         *                       value here indicates no limit.
         * @return A new SqlExtractorProvider with modified configuration.
         */
        public SqsExtractorProvider<T> withBatchSizeLimit(Integer batchSizeLimit) {
            return new SqsExtractorProvider<>(sqsClient, queueUrl, stringMapper, batchSizeLimit);
        }

        /**
         * Constructs an instance of an SQSExtractor based on the current configuration set.
         * @return An SQSExtractor instance.
         */
        @Override
        public SqsExtractor<T> get() {
            AmazonSQS effectiveClient = (sqsClient == null) ? DEFAULT_SQS_CLIENT_BUILDER.get() : sqsClient;
            return new SqsExtractor<>(effectiveClient, stringMapper, queueUrl, batchSizeLimit);
        }
    }

    private SqsExtractor(
            AmazonSQS amazonSqs,
            Function<String, T> stringMapper,
            String queueUrl,
            Integer batchSizeLimit) {
        this.amazonSqs = amazonSqs;
        this.stringMapper = stringMapper;
        this.queueUrl = queueUrl;
        this.extractedMessagesReceiptHandle = new ArrayList<>(0);
        this.batchSizeLimit = (batchSizeLimit == null || batchSizeLimit < 0) ? Integer.MAX_VALUE : batchSizeLimit;
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
    }

    /**
     * Extract next object from SQS.
     * @return The next object or empty if no more objects can be extracted.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     *                                             detected and the stream needs to be aborted.
     */
    @Override
    public Optional<T> next() throws UnrecoverableStreamFailureException {
        if (isClosed) {
            IllegalStateException e = new IllegalStateException("Attempt to use extractor that has been closed");
            logger.error("Error inside extractor: ", e);
            throw e;
        }

        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "SqsExtractor.next")) {
            Iterator<Message> messageIterator = getMessageIterator();
            if (messageIterator.hasNext()) {
                try {
                    Message extractedMessage = messageIterator.next();
                    T extractedObject = stringMapper.apply(extractedMessage.getBody());
                    extractedMessagesReceiptHandle.add(extractedMessage.getReceiptHandle());
                    scope.addCounter("SqsExtractor.extractionSuccess", 1);
                    scope.addCounter("SqsExtractor.extractionFailure", 0);
                    return Optional.of(extractedObject);
                } catch (RuntimeException retryableException) {
                    scope.addCounter("SqsExtractor.extractionSuccess", 0);
                    scope.addCounter("SqsExtractor.extractionFailure", 1);
                    throw retryableException;
                }
            }
        }
        return Optional.empty();
    }

    private Iterator<Message> getMessageIterator() throws UnrecoverableStreamFailureException {
        if (messageIterator == null) {
            // Need to use Set since calling receiveMessage in loop might return duplicate messages from SQS.
            Set<Message> messageSet = new LinkedHashSet<>();

            // Keep trying until SQS returns no messages or we hit the batchSizeLimit
            while (extractedMessagesReceiptHandle.size() + messageSet.size() < batchSizeLimit) {
                List<Message> messages = null;
                int retries;
                for (retries = 0; retries < SQS_GET_MESSAGES_MAX_RETRIES; retries++) {
                    try {
                        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                                .withQueueUrl(queueUrl)
                                .withWaitTimeSeconds(SQS_LONG_POLLING_PERIOD_IN_SECONDS)
                                // Never try and retrieve more messages than we are contracted to handle in total
                                .withMaxNumberOfMessages(
                                        Math.min(batchSizeLimit - (extractedMessagesReceiptHandle.size() + messageSet.size()),
                                                SQS_MAXIMUM_MESSAGES_PER_REQUEST));
                        messages = amazonSqs.receiveMessage(receiveMessageRequest).getMessages();
                        break;
                    } catch (AmazonServiceException e) {
                        logger.warn("Problem retrieving messages. Retrying...", e);
                        logger.warn(String.format("Retries count: %d", retries));
                    } catch (AmazonClientException e) {
                        logger.error("Non-retriable exception received", e);
                        throw new UnrecoverableStreamFailureException(e);
                    }
                }

                if (retries >= SQS_GET_MESSAGES_MAX_RETRIES) {
                    throw new UnrecoverableStreamFailureException("Maximum number of retries reached attempting to get "
                                                                  + "SQS messages. Giving up.");
                }

                if (messages.isEmpty()) break;
                messageSet.addAll(messages);
            }

            messageIterator = messageSet.iterator();
        }

        return messageIterator;
    }

    /**
     * Deletes all the extracted messages from SQS and emits time metrics.
     *
     * @throws Exception if something goes wrong
     */
    @Override
    public void close() throws Exception {
        isClosed = true;
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SqsExtractor.close")) {
            for(String receiptHandle : extractedMessagesReceiptHandle) {
                amazonSqs.deleteMessage(queueUrl, receiptHandle);
            }
        }
    }
}