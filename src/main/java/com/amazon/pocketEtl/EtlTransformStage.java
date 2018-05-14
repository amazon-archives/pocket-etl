/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;


import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import com.amazon.pocketEtl.core.consumer.EtlConsumerFactory;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static org.apache.logging.log4j.LogManager.getLogger;

@Getter(AccessLevel.PACKAGE)
@EqualsAndHashCode(callSuper = true)
class EtlTransformStage<T> extends EtlConsumerStage<T> {
    private final static String DEFAULT_TRANSFORM_STAGE_NAME = "EtlStream.Transform";

    private final static EtlExecutorFactory defaultExecutorFactory = new EtlExecutorFactory();
    private final static EtlConsumerFactory defaultConsumerFactory = new EtlConsumerFactory(defaultExecutorFactory);

    private final Transformer<T, ?> transformer;
    private final EtlExecutorFactory etlExecutorFactory;
    private final EtlConsumerFactory etlConsumerFactory;

    EtlTransformStage(@Nonnull Class<T> classForStage,
                              @Nonnull Transformer<T, ?> transformer,
                              @Nonnull String stageName,
                              @Nonnull Integer numberOfThreads,
                              @Nonnull Function<T, String> objectLogger,
                              @Nonnull EtlExecutorFactory etlExecutorFactory,
                              @Nonnull EtlConsumerFactory etlConsumerFactory) {
        super(classForStage, stageName, numberOfThreads, objectLogger);
        this.transformer = transformer;
        this.etlExecutorFactory = etlExecutorFactory;
        this.etlConsumerFactory = etlConsumerFactory;
    }

    @Override
    public EtlTransformStage<T> withObjectLogger(@Nonnull Function<T, String> objectLogger) {
        return new EtlTransformStage<>(getClassForStage(), getTransformer(), getStageName(), getNumberOfThreads(),
                objectLogger, getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    @Override
    public EtlTransformStage<T> withName(@Nonnull String stageName) {
        return new EtlTransformStage<>(getClassForStage(), getTransformer(), stageName, getNumberOfThreads(),
                getObjectLogger(), getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    @Override
    public EtlTransformStage<T> withThreads(@Nonnull Integer threads) {
        return new EtlTransformStage<>(getClassForStage(), getTransformer(), getStageName(), threads, getObjectLogger(),
                getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    static <T> EtlTransformStage<T> of(@Nonnull Class<T> classForStage, @Nonnull Transformer<T,?> transformer) {
        return new EtlTransformStage<>(classForStage, transformer, DEFAULT_TRANSFORM_STAGE_NAME,
                getDefaultNumberOfWorkers(), getDefaultObjectLogger(), defaultExecutorFactory, defaultConsumerFactory);
    }

    @Override
    EtlConsumer constructConsumerForStage(EtlConsumer downstreamConsumer) {
        if (downstreamConsumer == null) {
            throw new IllegalArgumentException("Attempt to construct transform stage with null downstream consumer");
        }

        EtlExecutor stageExecutor = getEtlExecutorFactory().newBlockingFixedThreadsEtlExecutor(getNumberOfThreads(),
                getDefaultQueueSize());
        EtlConsumer errorConsumer = getEtlConsumerFactory().newLogAsErrorConsumer(getStageName(), getLogger(getStageName()),
                getClassForStage(), getObjectLogger());

        return getEtlConsumerFactory().newTransformer(getStageName(), getTransformer(), getClassForStage(),
                downstreamConsumer, errorConsumer, stageExecutor);
    }

    @Override
    boolean isTerminal() {
        return false;
    }
}
