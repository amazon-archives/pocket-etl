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

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.DefaultLoggingStrategy;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Abstract class that represents a consumer stage in an EtlStream. An EtlStream consists of a producer stage chained
 * to one or more consumer stages.
 * @param <T> The type of the data being operated on by this stage in the stream.
 */
@Getter
public abstract class EtlConsumerStage<T> {
    /**
     * Static constructor for an EtlConsumerStage that transforms data in the stream. Used as a component in an
     * EtlStream.
     * @param classForStage The class that represents a view of the data to be operated on in the stream for this stage.
     * @param transformer A transformer that transforms the data on the stream into another form.
     * @param <T> Inferred type for the stage based on the classForStage.
     * @return An EtlConsumerStage that can be used as a component for an EtlStream.
     */
    public static <T> EtlConsumerStage<T> transform(@Nonnull Class<T> classForStage,
                                                     @Nonnull Transformer<T,?> transformer) {
        return EtlTransformStage.of(classForStage, transformer);
    }

    /**
     * Static constructor for an EtlConsumerStage that loads data to a final destination and terminates the stream. Used
     * as a component in an EtlStream.
     * @param classForStage The class that represents a view of the data to be operated on in the stream for this stage.
     * @param loader A loader that loads data from the stream to a final destination.
     * @param <T> Inferred type for the stage based on the classForStage.
     * @return An EtlConsumerStage that can be used as a component for an EtlStream.
     */
    public static <T> EtlConsumerStage<T> load(@Nonnull Class<T> classForStage,
                                           @Nonnull Loader<T> loader) {
        return EtlLoadStage.of(classForStage, loader);
    }

    /**
     * Construct a new EtlConsumerStage object that is the copy of an existing one but with a new specific value.
     * @param objectLogger Function to create a string representation of the object to be logged.
     * @return A new EtlConsumerStage object
     */
    public abstract EtlConsumerStage<T> withObjectLogger(@Nonnull Function<T, String> objectLogger);

    /**
     * Construct a new EtlConsumerStage object that is the copy of an existing one but with a new specific value.
     * @param stageName This is a human readable name for the stage that is used in profiling and logging and will help you
     *             build specific monitors and alarms for specific jobs or stages of those jobs.
     * @return A new EtlConsumerStage object.
     */
    public abstract EtlConsumerStage<T> withName(@Nonnull String stageName);

    /**
     * Construct a new EtlConsumerStage object that is the copy of an existing one but with a new specific value.
     * @param threads Every stage by default has a single worker. You can increase the number of parallel workers by
     *                specifying a different value, but if you do so make sure that the wrapped transformation or load
     *                that operates on the stream is threadsafe.
     * @return A new EtlConsumerStage object.
     */
    public abstract EtlConsumerStage<T> withThreads(@Nonnull Integer threads);

    /****************************************************************************************************************/

    private final static int DEFAULT_QUEUE_SIZE = 1000;
    private final static int DEFAULT_NUMBER_OF_WORKERS = 1;

    private final String stageName;
    private final Integer numberOfThreads;
    private final Class<T> classForStage;
    private final Function<T, String> objectLogger;

    static int getDefaultQueueSize() {
        return DEFAULT_QUEUE_SIZE;
    }

    static int getDefaultNumberOfWorkers() {
        return DEFAULT_NUMBER_OF_WORKERS;
    }

    static <T> Function<T, String> getDefaultObjectLogger() {
        return new DefaultLoggingStrategy<>();
    }

    EtlConsumerStage(@Nonnull Class<T> classForStage,
                     @Nonnull String stageName,
                     @Nonnull Integer numberOfThreads,
                     @Nonnull Function<T, String> objectLogger) {
        this.stageName = stageName;
        this.numberOfThreads = numberOfThreads;
        this.classForStage = classForStage;
        this.objectLogger = objectLogger;
    }

    abstract EtlConsumer constructConsumerForStage(EtlConsumer downstreamConsumer);
    abstract boolean isTerminal();


}
