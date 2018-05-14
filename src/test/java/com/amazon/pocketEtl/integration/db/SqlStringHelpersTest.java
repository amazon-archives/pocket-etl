/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.integration.db;

import com.google.common.collect.ImmutableList;
import javafx.util.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SqlStringHelpersTest {

    private static final String INPUT_SQL = "SELECT * FROM User WHERE firstName = ${firstName} AND lastName = ${lastName}";
    private static final Pair<String, String> FIRST_NAME_SUBSTITUTION = new Pair<>("firstName", "'Billy'");
    private static final Pair<String, String> LAST_NAME_SUBSTITUTION = new Pair<>("lastName", "'The Kid'");
    private static final Pair<String, String> OCCUPATION_SUBSTITUTION = new Pair<>("occupation", "'outlaw'");

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
        List<Pair<String, String>> substitutions = ImmutableList.of(FIRST_NAME_SUBSTITUTION);
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = ${lastName}";

        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutions);

        assertThat(result, is(expectedSQL));
    }

    @Test
    public void substituteSQLForMultipleSubstitutionsWillReplaceKeyWithValue() {
        List<Pair<String, String>> substitutions = ImmutableList.of(FIRST_NAME_SUBSTITUTION, LAST_NAME_SUBSTITUTION);
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = 'The Kid'";

        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutions);

        assertThat(result, is(expectedSQL));
    }

    @Test
    public void substituteSQLReturnsUnmodifiedSQLGivenEmptyList() {
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, new ArrayList<>());
        assertThat(result, is(INPUT_SQL));
    }

    @Test
    public void substituteSQLReturnsUnmodifiedSQLGivenNullList() {
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, null);
        assertThat(result, is(INPUT_SQL));
    }

    @Test
    public void substituteSQLDoesNotModifyKeyIfSubstitutionKeyDoesNotFindMatch() {
        List<Pair<String, String>> substitutionList = ImmutableList.of(
                FIRST_NAME_SUBSTITUTION,
                OCCUPATION_SUBSTITUTION
        );
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = ${lastName}";
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutionList);
        assertThat(result, is(expectedSQL));
    }

    @Test
    public void substituteSQLDoesNotModifyKeyIfSubstitutionIsNull() {
        List<Pair<String, String>> substitutionList = Arrays.asList(FIRST_NAME_SUBSTITUTION, null);
        String expectedSQL = "SELECT * FROM User WHERE firstName = 'Billy' AND lastName = ${lastName}";
        String result = SqlStringHelpers.substituteSQL(INPUT_SQL, substitutionList);
        assertThat(result, is(expectedSQL));
    }
}
