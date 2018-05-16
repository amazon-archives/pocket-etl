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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SqlStringHelpersTest {

    private static final String INPUT_SQL = "SELECT * FROM User WHERE firstName = ${firstName} AND lastName = ${lastName}";
    private static final Map<String, String> FIRST_NAME_SUBSTITUTION = ImmutableMap.of("firstName", "'Billy'");
    private static final Map<String, String> LAST_NAME_SUBSTITUTION = ImmutableMap.of("lastName", "'The Kid'");
    private static final Map<String, String> OCCUPATION_SUBSTITUTION = ImmutableMap.of("occupation", "'outlaw'");

    @Test
    public void listToSqlStringWithPopulatedListReturnsCommaSeparatedStrings() {
        List<String> listOfStrings = ImmutableList.of("a", "b", "c");
        String expected = "'a', 'b', 'c'";
        String actual = SqlStringHelpers.listToSQLString(listOfStrings);
        assertThat(actual, is(expected));
    }

    @Test
    public void listToSqlStringWithSingleElementInListReturnsSingleQuotesNoCommas() {
        List<String> singleStringList = ImmutableList.of("a");
        String expected = "'a'";
        String actual = SqlStringHelpers.listToSQLString(singleStringList);
        assertThat(actual, is(expected));
    }

    @Test
    public void listToSqlStringReturnsEmptyStringIfPassedNull() {
        String expected = "";
        String actual = SqlStringHelpers.listToSQLString(null);
        assertThat(actual, is(expected));
    }

    @Test
    public void listToSqlStringReturnsEmptyStringIfPassedEmptyList() {
        String expected = "";
        String actual = SqlStringHelpers.listToSQLString(new ArrayList<>());
        assertThat(actual, is(expected));
    }

    @Test
    public void substituteSQLForSingleSubstitutionWillReplaceKeyWithValue() {
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = ${lastName}";

        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, FIRST_NAME_SUBSTITUTION);

        assertThat(result, is(expectedSQL));
    }

    @Test
    public void substituteSQLForMultipleSubstitutionsWillReplaceKeyWithValue() {

        Map<String, String> substitutions = ImmutableMap.<String,String>builder().putAll(FIRST_NAME_SUBSTITUTION)
                .putAll(LAST_NAME_SUBSTITUTION).build();
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = 'The Kid'";

        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutions);

        assertThat(result, is(expectedSQL));
    }

    @Test
    public void substituteSQLReturnsUnmodifiedSQLGivenEmptyList() {
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, ImmutableMap.of());
        assertThat(result, is(INPUT_SQL));
    }

    @Test
    public void substituteSQLReturnsUnmodifiedSQLGivenNullList() {
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, null);
        assertThat(result, is(INPUT_SQL));
    }

    @Test
    public void substituteSQLDoesNotModifyKeyIfSubstitutionKeyDoesNotFindMatch() {
        Map<String, String> substitutions = ImmutableMap.<String,String>builder().putAll(FIRST_NAME_SUBSTITUTION)
                .putAll(OCCUPATION_SUBSTITUTION).build();

        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = ${lastName}";
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutions);
        assertThat(result, is(expectedSQL));
    }
}
