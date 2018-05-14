/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
