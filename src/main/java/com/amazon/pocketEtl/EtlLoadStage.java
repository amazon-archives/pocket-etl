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
class EtlLoadStage<T> extends EtlConsumerStage<T> {
    private final static String DEFAULT_LOAD_STAGE_NAME = "EtlStream.Load";

    private final static EtlExecutorFactory defaultExecutorFactory = new EtlExecutorFactory();
    private final static EtlConsumerFactory defaultConsumerFactory = new EtlConsumerFactory(defaultExecutorFactory);

    private final Loader<T> loader;
    private final EtlExecutorFactory etlExecutorFactory;
    private final EtlConsumerFactory etlConsumerFactory;

    EtlLoadStage(@Nonnull Class<T> classForStage,
                         @Nonnull Loader<T> loader,
                         @Nonnull String stageName,
                         @Nonnull Integer numberOfThreads,
                         @Nonnull Function<T, String> objectLogger,
                         @Nonnull EtlExecutorFactory etlExecutorFactory,
                         @Nonnull EtlConsumerFactory etlConsumerFactory) {
        super(classForStage, stageName, numberOfThreads, objectLogger);
        this.loader = loader;
        this.etlExecutorFactory = etlExecutorFactory;
        this.etlConsumerFactory = etlConsumerFactory;
    }

    @Override
    public EtlLoadStage<T> withObjectLogger(@Nonnull Function<T, String> objectLogger) {
        return new EtlLoadStage<>(getClassForStage(), getLoader(), getStageName(), getNumberOfThreads(), objectLogger,
                getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    @Override
    public EtlLoadStage<T> withName(@Nonnull String stageName) {
        return new EtlLoadStage<>(getClassForStage(), getLoader(), stageName, getNumberOfThreads(), getObjectLogger(),
                getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    @Override
    public EtlLoadStage<T> withThreads(@Nonnull Integer threads) {
        return new EtlLoadStage<>(getClassForStage(), getLoader(), getStageName(), threads, getObjectLogger(),
                getEtlExecutorFactory(), getEtlConsumerFactory());
    }

    static <T> EtlLoadStage<T> of(@Nonnull Class<T> classForStage, @Nonnull Loader<T> loader) {
        return new EtlLoadStage<>(classForStage, loader, DEFAULT_LOAD_STAGE_NAME, getDefaultNumberOfWorkers(),
                getDefaultObjectLogger(), defaultExecutorFactory, defaultConsumerFactory);
    }

    @Override
    EtlConsumer constructConsumerForStage(EtlConsumer downstreamConsumer) {
        EtlExecutor stageExecutor = getEtlExecutorFactory().newBlockingFixedThreadsEtlExecutor(getNumberOfThreads(),
                getDefaultQueueSize());
        EtlConsumer errorConsumer = getEtlConsumerFactory().newLogAsErrorConsumer(getStageName(), getLogger(getStageName()),
                getClassForStage(), getObjectLogger());

        return getEtlConsumerFactory().newLoader(getStageName(), getLoader(), getClassForStage(), errorConsumer, stageExecutor);
    }

    @Override
    boolean isTerminal() {
        return true;
    }
}
