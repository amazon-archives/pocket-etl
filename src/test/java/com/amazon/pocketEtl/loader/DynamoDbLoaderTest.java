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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDbLoaderTest {
    @Mock
    private DynamoDB ddbMock;

    @Mock
    private EtlMetrics metricsMock;

    @Mock
    private Table tableMock;

    @Mock
    private ObjectWriter writerMock;

    @Captor
    private ArgumentCaptor<Item> tablePutItemCaptor;

    @Before
    public void initializeMetrics() {
        when(metricsMock.createChildMetrics()).thenReturn(metricsMock);
    }

    @Test
    public void testSuccess() throws Exception {
        final String tableName = "itsatableyo";
        final String tableHashKey = "id";
        final String pk = "peep";

        when(ddbMock.getTable(tableName)).thenReturn(tableMock);

        DynamoDbLoader<Thing> loader = DynamoDbLoader.of(tableName, tableHashKey, Thing::getId).withDynamoDb(ddbMock);

        // SUT
        loader.open(metricsMock);
        loader.load(new Thing(pk, "2001", Lists.newArrayList(1, 4, 9)));
        loader.close();

        verify(metricsMock).addCount(eq("DynamoDbLoader.success"), eq(1d));
        verify(metricsMock).addCount(eq("DynamoDbLoader.failure"), eq(0d));

        verify(tableMock).putItem(tablePutItemCaptor.capture());

        assertThat(tablePutItemCaptor.getValue().get(tableHashKey), is(pk));
        final Map<String, Object> document = (Map<String, Object>) tablePutItemCaptor.getValue().get("document");
        assertThat(document, hasEntry("id", "peep"));
        assertThat(document, hasEntry("year", "2001"));
        assertThat(document, hasKey("stuff"));
        assertThat((Collection<BigDecimal>) document.get("stuff"), hasItems(BigDecimal.valueOf(1), BigDecimal.valueOf(4), BigDecimal.valueOf(9)));
    }

    @Test
    public void failToPut() throws Exception {
        final String tableName = "itsatableyo";
        final String tableHashKey = "id";
        final String pk = "peep";

        when(ddbMock.getTable(tableName)).thenReturn(tableMock);
        when(tableMock.putItem(any(Item.class))).thenThrow(new RuntimeException("failed"));

        DynamoDbLoader<Thing> loader = DynamoDbLoader.of(tableName, tableHashKey, Thing::getId).withDynamoDb(ddbMock);

        // SUT
        loader.open(metricsMock);
        loader.load(new Thing(pk, "2001", Lists.newArrayList(1, 4, 9)));
        loader.close();

        verify(metricsMock).addCount(eq("DynamoDbLoader.success"), eq(0d));
        verify(metricsMock).addCount(eq("DynamoDbLoader.failure"), eq(1d));
    }

    @Test
    public void failToExtractPk() throws Exception {
        final String tableName = "itsatableyo";
        final String tableHashKey = "id";
        final String pk = "peep";

        DynamoDbLoader<Thing> loader = DynamoDbLoader.of(tableName, tableHashKey, (Thing x) -> {
            throw new RuntimeException("Problem extracting");
        }).withDynamoDb(ddbMock);

        // SUT
        loader.open(metricsMock);
        loader.load(new Thing(pk, "2001", Lists.newArrayList(1, 4, 9)));
        loader.close();

        verify(metricsMock).addCount(eq("DynamoDbLoader.success"), eq(0d));
        verify(metricsMock).addCount(eq("DynamoDbLoader.failure"), eq(1d));
    }

    @Test
    public void failToParseJson() throws Exception {
        final String tableName = "itsatableyo";
        final String tableHashKey = "id";
        final String pk = "peep";

        when(writerMock.writeValueAsString(any(Object.class))).thenThrow(new JsonParseException(null, "💣"));

        DynamoDbLoader<Thing> loader = DynamoDbLoader.of(tableName, tableHashKey, Thing::getId).withDynamoDb(ddbMock).withWriter(writerMock);

        // SUT
        loader.open(metricsMock);
        loader.load(new Thing(pk, "2001", Lists.newArrayList(1, 4, 9)));
        loader.close();

        verify(metricsMock).addCount(eq("DynamoDbLoader.success"), eq(0d));
        verify(metricsMock).addCount(eq("DynamoDbLoader.failure"), eq(1d));
    }

    static class Thing {
        public String id;
        public String year;
        public List<Integer> stuff = Lists.newArrayList();

        Thing(String id, String year, List<Integer> stuff) {
            this.id = id;
            this.year = year;
            this.stuff.addAll(stuff);
        }

        public String getId() {
            return id;
        }
    }
}