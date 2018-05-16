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

package com.amazon.pocketEtl.integration.db;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
    public static String substituteSQL(String originalSql, Map<String, String> keyValueSubstitutions) {
        if (keyValueSubstitutions == null) { return originalSql; }

        AtomicReference<String> result = new AtomicReference<>(originalSql);

        keyValueSubstitutions.forEach((key, value) -> {
            String substitutionKey = String.format("${%s}", key);
            result.set(result.get().replace(substitutionKey, value));
        });

        return result.get();
    }
}