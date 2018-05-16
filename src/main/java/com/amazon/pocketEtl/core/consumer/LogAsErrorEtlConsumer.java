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

package com.amazon.pocketEtl.core.consumer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.core.EtlStreamObject;
import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Implementation of Consumer that logs the string representation of any objects it consumes to a log4j logger as an
 * error.
 *
 * @param <T> Type of object to be logged
 */
@EqualsAndHashCode
class LogAsErrorEtlConsumer<T> implements EtlConsumer {
    private final String name;
    private final Logger errorLogger;
    private EtlMetrics parentMetrics;
    private Class<T> dtoClass;
    private Function<T, String> loggingStrategy;

    /**
     * Constructor that creates consumer with logging strategy
     *
     * @param name            A human readable name for the instance of this class that will be used in logging and metrics.
     * @param errorLogger     Apache log4j logger object to log errors to.
     * @param dtoClass        Class object to use to create the returned object.
     * @param loggingStrategy Function that takes an object from the ETL stream and returns a string representation of that object to be logged.
     */
    LogAsErrorEtlConsumer(String name, Logger errorLogger, @Nonnull Class<T> dtoClass, @Nonnull Function<T, String> loggingStrategy) {
        this.name = name;
        this.errorLogger = errorLogger;
        this.dtoClass = dtoClass;
        this.loggingStrategy = loggingStrategy;
    }

    /**
     * Consumes and logs the string representation of a single object as an error.
     *
     * @param objectToConsume The object to be consumed.
     * @throws IllegalStateException If this consumer is not in a state able to do work.
     */
    @Override
    public void consume(EtlStreamObject objectToConsume) throws IllegalStateException {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "LogAsErrorConsumer." + name + ".consume")) {
            String logMessage = loggingStrategy.apply(objectToConsume.get(dtoClass));
            errorLogger.error("ETL failure for object: " + logMessage);
        } catch (RuntimeException e) {
            errorLogger.error("ETL failure for object type '" + objectToConsume.getClass().getSimpleName() +
                    "'. Logging Strategy failed with exception: ", e);
        }
    }

    /**
     * Signal the consumer to prepare to accept work. Does nothing in this implementation.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
        new EtlProfilingScope(parentMetrics, "LogAsErrorConsumer." + name + ".open").close();
    }

    /**
     * Signal the consumer to wrap up any buffered work and close and finalize streams. Does nothing in this
     * implementation.
     *
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        new EtlProfilingScope(parentMetrics, "LogAsErrorConsumer." + name + ".close").close();
    }
}
