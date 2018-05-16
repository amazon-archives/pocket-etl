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

package com.amazon.pocketEtl.core;

import lombok.EqualsAndHashCode;

import java.util.function.Function;

/**
 * This Logging Strategy is applied if no Logging Strategy was provided by the user.
 * @param <T> Type of the object to be logged
 */
@EqualsAndHashCode
public class DefaultLoggingStrategy<T> implements Function<T, String> {

    private static final String DEFAULT_LOG_MESSAGE = "For more detailed logging for the object that failed, provide a "
            + "custom logging strategy to the EtlStage.";

    @Override
    public String apply(T t) {
        return DEFAULT_LOG_MESSAGE;
    }
}
