/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.integration.db;

import javafx.util.Pair;

import java.util.List;

/**
 * An Helper class which exposes static methods for substituting values in SQL string which
 * normally can't be done with the APIs that are currently being used in the package.
 */
public class SqlStringHelpers {

    /**
     * Creates a comma separated string-list, with each element wrapped in single quotes.
     *
     * @param stringList - A list of Strings (e.g. ["a", "b", "c"] )
     * @return A comma separated string-list (e.g. "'a', 'b', 'c'" )
     */
    public static String listToSQLString(List<String> stringList) {
        if (stringList == null) { return ""; }

        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;

        for (String value : stringList) {
            if (!isFirst) {
                stringBuilder.append(", ");
            }

            stringBuilder.append('\'').append(value).append('\'');
            isFirst = false;
        }

        return stringBuilder.toString();
    }

    /**
     * Replaces key with corresponding value in the SQL string.
     * Keys are enclosed by ${} in SQL string. For e.g:
     * firstName is the key in "SELECT * from User where firstName=${firstName}".
     *
     * @param originalSql SQL string having keys that needs to be replaced with the corresponding value.
     * @param keyValueSubstitutions Key-Value pairs.
     * @return An SQL string with substitutions filled in.
     */
    public static String substituteSQL(String originalSql, List<Pair<String, String>> keyValueSubstitutions) {
        if (keyValueSubstitutions == null) { return originalSql; }

        String result = originalSql;

        for (Pair<String, String> substitution : keyValueSubstitutions) {
            if (substitution == null) { continue; }
            String substitutionKey = String.format("${%s}", substitution.getKey());
            result = result.replace(substitutionKey, substitution.getValue());
        }

        return result;
    }
}