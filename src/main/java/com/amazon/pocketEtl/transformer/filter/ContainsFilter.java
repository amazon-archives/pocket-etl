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

package com.amazon.pocketEtl.transformer.filter;

import com.amazon.pocketEtl.lookup.Lookup;

import java.util.function.BiPredicate;

/**
 * Implementation of a filter that tests to see if the filterSet contains an object equal to the one being filtered.
 * @param <T> The type of object being filtered.
 */
public class ContainsFilter<T> implements BiPredicate<T, Lookup<T, T>> {
    /**
     * Tests to see if an equal version of the object can be found in the filterSet.
     * @param objectToFilter The object being filtered.
     * @param filterSet A lookup containing the values to compare the filtered object against.
     * @return true if an object of equivalent value is found in the filterSet; or false if it is not.
     */
    @Override
    public boolean test(T objectToFilter, Lookup<T, T> filterSet) {
        return filterSet.get(objectToFilter).isPresent();
    }
}
