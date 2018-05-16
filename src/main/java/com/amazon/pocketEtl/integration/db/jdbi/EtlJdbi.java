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

package com.amazon.pocketEtl.integration.db.jdbi;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.HashPrefixStatementRewriter;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * JDBI integration factory. Constructs a DBI that has custom handlers for things we care about:
 * - Joda DateTime marshalling from ResultSet.
 * - String list SQL parameter handling (Postgres/Redshift only)
 */
public class EtlJdbi {
    /**
     * Construct a new DBI wrapper for a JDBC datasource using an optional secondary mapper. The secondary mapper comes
     * into play when the default mapper is unable to directly map a value that the SQL query returned to a property in
     * the bean class you have specified. In this case you give it a lambda to call that will insert the value correctly
     * into the bean object.
     * @param dataSource JDBC datasource to be wrapped.
     * @param secondaryMapper A lambda that is invoked when a data element can't be directly mapped to the bean is
     *                        extracted. The lambda takes as its arguments the bean that is being extracted to and
     *                        a Map.Entry that represents the (key, value) pair of the data element that could not be
     *                        directly mapped to the bean. If set to null, unrecognized properties will be ignored.
     * @return A fully constructed DBI object.
     */
    public static <T> DBI newDBI(DataSource dataSource, @Nullable BiConsumer<T, Map.Entry<String, String>> secondaryMapper) {
        DBI dbi = new DBI(dataSource);
        dbi.setStatementRewriter(new HashPrefixStatementRewriter());
        dbi.registerMapper(new EtlBeanMapperFactory(secondaryMapper));
        dbi.registerArgumentFactory(new PostgresStringArrayArgumentFactory());

        return dbi;
    }
}
