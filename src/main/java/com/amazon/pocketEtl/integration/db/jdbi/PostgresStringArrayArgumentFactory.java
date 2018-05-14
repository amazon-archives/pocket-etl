/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.integration.db.jdbi;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.Array;
import java.util.List;

/**
 * JDBI ArgumentFactory implementation that takes a string list argument and uses JDBC createArrayOf to pass it in as a
 * preparedStatement parameter.
 * CAUTION: This particular implementation is Postgres/Redshift specific and is not believed to be portable to other
 * databases.
 */
class PostgresStringArrayArgumentFactory implements ArgumentFactory<List<String>> {
    @Override
    public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
        // Check if the object is a list
        if (!List.class.isAssignableFrom(value.getClass())) {
            return false;
        }

        List<?> untypedList = (List) value;

        // Check that each object in the list can be assigned into a String
        for (Object obj : untypedList) {
            if (!String.class.isAssignableFrom(obj.getClass())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Argument build(Class<?> expectedType,
                          final List<String> value,
                          StatementContext ctx) {
        return (position, statement, ctx1) -> {
            // in postgres no need to (and in fact cannot) free arrays
            Array ary = ctx1.getConnection()
                    .createArrayOf("text", value.toArray());
            statement.setArray(position, ary);
        };
    }
}

