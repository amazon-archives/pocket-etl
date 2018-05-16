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

package functionalTests;

import com.amazon.pocketEtl.loader.DynamoDbLoader;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DynamoDbFunctionalTest {
    private AmazonDynamoDB ddb;

    private String tableName;

    @Before
    public void setup() {
        tableName = "etl001";

        ddb = DynamoDBEmbedded.create().amazonDynamoDB();

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(tableName)
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                .withAttributeDefinitions(new AttributeDefinition("pk", "S"))
                .withKeySchema(new KeySchemaElement("pk", "HASH"));

        ddb.createTable(createTableRequest);
    }

    @Test
    public void testHappyCaseLoading() {
        final DynamoDbLoader<Thing> loader = DynamoDbLoader.of(tableName, "pk", Thing::getSomeUniqueId).withClient(ddb);

        // add an item
        {
            assertThat(getThingFromDdb("monolith").getItem(), is(nullValue()));

            Thing t001 = new Thing();
            t001.someUniqueId = "monolith";
            t001.year = "2001";
            loader.load(t001);

            assertThat(getThingFromDdb("monolith").getItem().get("document").getM().get("year").getS(), is("2001"));
        }

        // add another item
        {
            assertThat(getThingFromDdb("macrolith").getItem(), is(nullValue()));

            Thing t001 = new Thing();
            t001.someUniqueId = "macrolith";
            t001.year = "2525";
            loader.load(t001);

            assertThat(getThingFromDdb("monolith").getItem().get("document").getM().get("year").getS(), is("2001"));
            assertThat(getThingFromDdb("macrolith").getItem().get("document").getM().get("year").getS(), is("2525"));
        }
    }

    private GetItemResult getThingFromDdb(String key) {
        final HashMap<String, AttributeValue> requestItems = new HashMap<>();
        requestItems.put("pk", new AttributeValue(key));
        final GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.withTableName(tableName).withKey(requestItems);
        return ddb.getItem(getItemRequest);
    }

    static class Thing {
        public String someUniqueId;
        public String year;

        public String getSomeUniqueId() {
            return someUniqueId;
        }

        public String getYear() {
            return year;
        }
    }
}
