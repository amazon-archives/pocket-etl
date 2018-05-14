/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.core.producer;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.prefs.BackingStoreException;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Implementation of producer that uses an Extractor object to produce new objects. Each produced object will then be
 * sent to a downstream Consumer.
 *
 * @param <T> The type of object produced by the Extractor.
 */
@EqualsAndHashCode
class ExtractorEtlProducer<T> implements EtlProducer {
    private final static Logger logger = getLogger(ExtractorEtlProducer.class);

    private final String name;
    private final EtlConsumer downstreamEtlConsumer;
    private final Extractor<T> extractor;
    private boolean isClosed = false;
    private EtlMetrics parentMetrics = null;

    /**
     * Standard constructor.
     *
     * @param downstreamEtlConsumer A consumer to send all the produced objects to.
     * @param extractor          An extractor object that will be used to produce new objects.
     */
    ExtractorEtlProducer(String name, EtlConsumer downstreamEtlConsumer, Extractor<T> extractor) {
        this.name = name;
        this.downstreamEtlConsumer = downstreamEtlConsumer;
        this.extractor = extractor;
    }

    /**
     * Run the extraction. The extractor will extractor all the objects that are available to be extracted and push those
     * objects to the consumer it is chained to. This method does not guarantee that all the extraction work will be
     * complete down the chain when it returns, only closing the extractor will do that.
     **/
    @Override
    public void produce() throws IllegalStateException, BackingStoreException {
        if (isClosed) {
            throw new IllegalStateException("Attempt to run extractor after extractor was closed");
        }

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExtractorProducer." + name + ".produce")) {
            Optional<T> result = Optional.empty();
            boolean nonFatalExceptionWasThrown;

            do {
                nonFatalExceptionWasThrown = false;

                try {
                    result = extractor.next();
                    result.ifPresent(obj -> downstreamEtlConsumer.consume(new EtlStreamObject().with(obj)));
                } catch (RuntimeException e) {
                    nonFatalExceptionWasThrown = true;
                    logger.error("Extractor threw an exception during next() operation:", e);
                }
            } while (nonFatalExceptionWasThrown || result.isPresent());
        }
    }

    /**
     * Signals the extractor that it should finish processing any buffered work and close its downstream consumer.
     *
     * @throws Exception If something went wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExtractorProducer." + name + ".close")) {
            isClosed = true;
            downstreamEtlConsumer.close();

            try {
                extractor.close();
            } catch (RuntimeException e) {
                logger.warn("Exception thrown closing extractor: ", e);
            }
        }
    }

    /**
     * Signals the producer that it should prepare to produce work. This will also signal the downstream consumer to
     * do the same.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ExtractorProducer." + name + ".open")) {
            extractor.open(parentMetrics);
            downstreamEtlConsumer.open(parentMetrics);
        }
    }
}