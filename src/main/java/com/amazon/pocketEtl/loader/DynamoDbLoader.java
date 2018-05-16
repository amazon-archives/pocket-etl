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

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Loader;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Function;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Loads a JSON record to a DynamoDB table. If an item with that hash key already exists in Dynamo, it will be
 * updated.
 * <p>
 **/

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DynamoDbLoader<T> implements Loader<T> {
    private final static String SUCCESS_METRIC_KEY = "DynamoDbLoader.success";
    private final static String FAILURE_METRIC_KEY = "DynamoDbLoader.failure";

    private final static Logger logger = getLogger(DynamoDbLoader.class);

    private final DynamoDB db;
    private final String tableName;
    private final String hashKey;
    private final Function<T, String> hashKeyExtractor;
    private final ObjectWriter writer;
    private EtlMetrics parentMetrics;

    public static <T> DynamoDbLoader<T> of(String tableName, String hashKey, Function<T, String> hashKeyExtractor) {
        return new DynamoDbLoader<>(new DynamoDB(AmazonDynamoDBClientBuilder.standard().withRegion("us-east-1").build()), tableName, hashKey, hashKeyExtractor, new ObjectMapper().writer());
    }

    public DynamoDbLoader<T> withDynamoDb(DynamoDB db) {
        return new DynamoDbLoader<>(db, tableName, hashKey, hashKeyExtractor, writer);
    }

    public DynamoDbLoader<T> withClient(AmazonDynamoDB ddbClient) {
        return new DynamoDbLoader<>(new DynamoDB(ddbClient), tableName, hashKey, hashKeyExtractor, writer);
    }

    // package-protected to allow for swapping out during unit testing
    DynamoDbLoader<T> withWriter(ObjectWriter writer) {
        return new DynamoDbLoader<>(db, tableName, hashKey, hashKeyExtractor, writer);
    }

    @Override
    public void load(T objectToLoad) {
        logger.debug("Loading");

        final String primaryKey;
        final String objectAsJson;

        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "DynamoDbLoader.prepare")) {
            try {
                primaryKey = hashKeyExtractor.apply(objectToLoad);
            } catch (RuntimeException e) {
                emitSuccessAndFailureMetrics(scope, false);
                logger.warn("Failed to extract primary key", e);
                return;
            }

            try {
                objectAsJson = writer.writeValueAsString(objectToLoad);
            } catch (IOException e) {
                emitSuccessAndFailureMetrics(scope, false);
                logger.warn("Failed to convert to JSON", e);
                return;
            }
        }

        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "DynamoDbLoader.load")) {
            Table table = db.getTable(tableName);
            Item item = new Item().withPrimaryKey(hashKey, primaryKey).withJSON("document", objectAsJson);

            // write to ddb. using a batch operation would improve performance
            try {
                table.putItem(item);
            } catch (Exception e) {
                emitSuccessAndFailureMetrics(scope, false);
                logger.warn("Failed to load record with primary key '{}'", primaryKey, e);
                return;
            }

            emitSuccessAndFailureMetrics(scope, true);
        }

        logger.debug("Loaded {} successfully", primaryKey);
    }

    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "DynamoDbLoader.open")) {
            this.parentMetrics = parentMetrics;
            logger.debug("Opening");
        }
    }

    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "DynamoDbLoader.close")) {
            logger.debug("Closing");
        }
    }

    private void emitSuccessAndFailureMetrics(final EtlProfilingScope scope, final boolean isSuccess) {
        scope.addCounter(SUCCESS_METRIC_KEY, isSuccess ? 1 : 0);
        scope.addCounter(FAILURE_METRIC_KEY, isSuccess ? 0 : 1);
    }
}