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

import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * An injectable factory class for building various types of useful consumers. This is the only way you should be
 * constructing consumers of any kind outside of this package. Typically consumers are actually chains :
 * SmartConsumer -> MetricsEmissionConsumer -> ExecutorConsumer -> SpecificConsumer
 * The SmartConsumer allows multiple upstream consumers to be chained to a single downstream consumer without causing
 * problems with open and close. This layer is invisible to the operation of the ETL stream, but necessary.
 * The ExecutorConsumer handles the parallelism of the consumer. If you don't want any parallelism, then pass in a
 * SingleThreadedEtlExecutor for the consumer bundles that require an EtlExecutor. Passing in a multithreaded EtlExecutor
 * means you have to ensure that the SpecificConsumer is threadsafe.
 */
@RequiredArgsConstructor
public class EtlConsumerFactory {
    private final EtlExecutorFactory etlExecutorFactory;

    /**
     * Constructs a consumer based on a Loader.
     * @param stageName The name of this consumer used in logging and reporting.
     * @param loader The loader object this consumer will be based on.
     * @param errorEtlConsumer A consumer to send all records that could not be loaded to.
     * @param etlExecutor An EtlExecutor object to handle parallelism for this consumer.
     * @param <T> The type of object being loaded.
     * @return A fully constructed consumer.
     */
    @Nonnull
    public <T> EtlConsumer newLoader(String stageName, Loader<T> loader, Class<T> loaderTypeClass,
                                            EtlConsumer errorEtlConsumer, EtlExecutor etlExecutor) {
        EtlConsumer loaderEtlConsumer = new LoaderEtlConsumer<>(stageName, loader, loaderTypeClass, errorEtlConsumer);

        return newWrappedConsumer(stageName, loaderEtlConsumer, etlExecutor);
    }

    /**
     * Constructs a consumer based on a Transformer.
     * @param stageName The name of this consumer used in logging and reporting.
     * @param transformer The transformer object this consumer will be based on.
     * @param downstreamEtlConsumer A consumer to pass in transformed objects to.
     * @param errorEtlConsumer A consumer to send all records that could not be transformed to.
     * @param etlExecutor An EtlExecutor object to handle parallelism for this consumer.
     * @param <Upstream> The type of objects that will be consumed by the transformer.
     * @param <Downstream> The type of objects that will be produced by the transformer.
     * @return A fully constructed consumer.
     */
    @Nonnull
    public <Upstream, Downstream> EtlConsumer newTransformer(
            String stageName,
            Transformer<Upstream, Downstream> transformer,
            Class<Upstream> transformerUpstreamTypeClass,
            EtlConsumer downstreamEtlConsumer,
            EtlConsumer errorEtlConsumer,
            EtlExecutor etlExecutor
    ) {
        EtlConsumer transformerEtlConsumer =
                new TransformerEtlConsumer<>(stageName, downstreamEtlConsumer, errorEtlConsumer, transformer,
                        transformerUpstreamTypeClass);

        return newWrappedConsumer(stageName, transformerEtlConsumer, etlExecutor);
    }

    /**
     * Constructs a consumer that will simply output the object to a log based on a logging strategy and then do
     * nothing further with it. Used for logging errors in your ETL flow.
     * @param stageName The name of this consumer used in logging and reporting.
     * @param errorLogger A logger object to log the objects to.
     * @param dtoClass Class object to use to create the returned object.
     * @param loggingStrategy Method to handle the logging of the object
     * @return A fully constructed consumer.
     */
    @Nonnull
    public <T> EtlConsumer newLogAsErrorConsumer(String stageName, Logger errorLogger, Class<T> dtoClass, Function<T, String> loggingStrategy) {
        return newWrappedConsumer(stageName + ".error", new LogAsErrorEtlConsumer<>(stageName, errorLogger, dtoClass, loggingStrategy),
                etlExecutorFactory.newImmediateExecutionEtlExecutor());
    }

    @Nonnull
    private EtlConsumer newWrappedConsumer(String stageName, EtlConsumer wrappedEtlConsumer,
                                                  EtlExecutor etlExecutor) {
        return new SmartEtlConsumer(stageName, new MetricsEmissionEtlConsumer(stageName,
                new ExecutorEtlConsumer(stageName, wrappedEtlConsumer, etlExecutor)));
    }
}
