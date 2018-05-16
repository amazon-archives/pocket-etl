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

import lombok.AllArgsConstructor;
import org.skife.jdbi.v2.ResultSetMapperFactory;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.util.Map;
import java.util.function.BiConsumer;

import static lombok.AccessLevel.PACKAGE;

/**
 * ResultSetMapperFactory implementation that will accept any class to be mapped and construct an EtlBeanMapper on
 * demand.
 */
@AllArgsConstructor(access = PACKAGE)
class EtlBeanMapperFactory implements ResultSetMapperFactory {
    private BiConsumer<?, Map.Entry<String, String>> secondaryMapper;

    @Override
    public boolean accepts(Class type, StatementContext ctx) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ResultSetMapper mapperFor(Class type, StatementContext ctx) {
        return new EtlBeanMapper<>(type, secondaryMapper);
    }
}
