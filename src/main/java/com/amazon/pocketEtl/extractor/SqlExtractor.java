/*
 *   Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.pocketEtl.extractor;

import static org.apache.logging.log4j.LogManager.getLogger;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;
import com.amazon.pocketEtl.integration.db.jdbi.EtlJdbi;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * An implementation of Extractor that uses a JDBC datasource as a backing store. JDBI is used to wrap the datasource
 * and provide SQL parameter parsing and substitution as well as marshalling into the target object.
 * <p>
 * The SQL paramaterization works by using named parameters with a hash prefix. Eg:
 * <p>
 * "SELECT columnOne FROM myTable WHERE someAttribute = #matchParameter"
 * <p>
 * For the parameter map you should pass in a map that associates a value to "matchParameter".
 *
 * @param <T> Type of object that is extracted from the datasource.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SqlExtractor<T> implements Extractor<T> {
    private final static Logger logger = getLogger(SqlExtractor.class);

    private final String extractSql;
    private final Class<T> extractClass;
    private final DataSource dataSource;
    private final Map<String, ?> extractSqlParameters;
    private final BiConsumer<T, Map.Entry<String, String>> unknownPropertyMapper;

    private boolean isClosed = false;
    private Handle handle = null;
    private ResultIterator<T> resultIterator = null;
    private EtlMetrics parentMetrics = null;

    public static <T> SqlExtractor<T> of(DataSource dataSource, String extractSql, Class<T> extractClass) {
        return new SqlExtractor<>(extractSql, extractClass, dataSource, null, null);
    }

    public SqlExtractor<T> withSqlParameters(Map<String, ?> extractSqlParameters) {
        return new SqlExtractor<>(extractSql, extractClass, dataSource, extractSqlParameters, unknownPropertyMapper);
    }

    public SqlExtractor<T> withUnknownPropertyMapper(BiConsumer<T, Map.Entry<String, String>> unknownPropertyMapper) {
        return new SqlExtractor<>(extractSql, extractClass, dataSource, extractSqlParameters, unknownPropertyMapper);
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
        DBI dbi = EtlJdbi.newDBI(dataSource, unknownPropertyMapper);
        handle = dbi.open();

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SqlExtractor.executeQuery")) {
            resultIterator = handle.createQuery(extractSql)
                    .bindFromMap(extractSqlParameters)
                    .mapTo(extractClass)
                    .iterator();
        }
    }

    /**
     * Extract the next object from the database.
     *
     * @return The next object or empty if no more objects can be extracted.
     * @throws UnrecoverableStreamFailureException An unrecoverable problem that affects the entire stream has been
     *                                             detected and the stream needs to be aborted.
     */
    @Override
    public Optional<T> next() throws UnrecoverableStreamFailureException {
        if (isClosed) {
            IllegalStateException e = new IllegalStateException("Attempt to use extractor that has been closed");
            logger.error("Error inside extractor: ", e);
            throw e;
        }

        if (resultIterator == null) {
            throw new IllegalStateException("Attempt to extract from an uninitialized extractor");
        }

        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "SqlExtractor.next")) {
            try {
                boolean hasNext;

                // If the JDBI's resultIterator hasNext() throws a RuntimeException, something bad happened.
                try {
                    hasNext = resultIterator.hasNext();
                } catch (RuntimeException e) {
                    throw new UnrecoverableStreamFailureException(e);
                }

                if (hasNext) {
                    T extractedObject = resultIterator.next();
                    scope.addCounter("SqlExtractor.extractionSuccess", 1);
                    scope.addCounter("SqlExtractor.extractionFailure", 0);
                    return Optional.of(extractedObject);
                }
            } catch (RuntimeException retryableException) {
                scope.addCounter("SqlExtractor.extractionSuccess", 0);
                scope.addCounter("SqlExtractor.extractionFailure", 1);
                throw retryableException;
            }
        }

        return Optional.empty();
    }

    /**
     * Frees up the resources this Extractor constructed for extracting the objects from the database.
     *
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        isClosed = true;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "SqlExtractor.close")) {
            if (resultIterator != null) {
                resultIterator.close();
            }

            if (handle != null) {
                handle.close();
            }
        }
    }
}