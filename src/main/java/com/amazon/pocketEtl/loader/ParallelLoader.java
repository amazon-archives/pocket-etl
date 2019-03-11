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

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Loader implementation that orchestrates running multiple distinct loaders in parallel. Which parallel loader gets which
 * work is arbitrary and based on the identity of the thread that performs load(). Each new thread the parallel loader
 * sees gets its own wrapped loader that will not be shared with other threads, therefore the wrapped loaders themselves
 * do not need to be threadsafe, but the work they do must not interfere with each other.
 *
 * An ideal use-case for this loader is streaming data out to files when IO is slow (for instance writing to S3) but
 * can be done in parallel without interference. Using the same S3 loader object with multiple threads would not work
 * in this case because the threads would have to synchronize on writes to the stream and you would therefore lose all
 * the potential throughput gains from parallelism.
 *
 * @param <T> Type of object being loaded.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ParallelLoader<T> implements Loader<T> {
    private final static Logger logger = getLogger(ParallelLoader.class);

    private final Supplier<Loader<T>> loaderSupplier;
    private final BiConsumer<Boolean, EtlMetrics> closeCallback;

    private boolean isClosed = false;
    private boolean dataWasLoaded = false;
    private EtlMetrics parentMetrics = null;

    // THREAD-SAFE OBJECTS: they are shared by and modified by concurrent threads
    private final Queue<Loader<T>> activeLoaders = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Loader<T>> loaderThreadLocal = new ThreadLocal<>();
    // END THREAD-SAFE

    /**
     * Create a new ParallelLoader based on a Loader factory. This factory will be used to dispense a new wrapped
     * Loader each time a new thread loading data is seen.
     * @param loaderSupplier An object that provides Loader objects.
     * @param <T> The type of object being Loaded.
     * @return A newly constructed ParallelLoader.
     */
    public static <T> ParallelLoader<T> of(Supplier<Loader<T>> loaderSupplier) {
        return new ParallelLoader<>(loaderSupplier, null);
    }

    /**
     * This allows a call-back method to be registered that will be invoked when close() is called on the ParallelLoader
     * and after all the wrapped Loaders have been subsequently closed. This allows for data 'stitching' or
     * post-processing to take place, for instance if you have used this loader to write a number of S3 files, you could
     * add a call-back here to load all those files into Redshift.
     * @param closeCallback Method that takes two arguments, a flag that indicates if any data was loaded ta all, and a
     *                      metrics object to attach counters and timers to.
     * @return A new copy of this loader with its behavior modified.
     */
    public ParallelLoader<T> withOnCloseCallback(BiConsumer<Boolean, EtlMetrics> closeCallback) {
        return new ParallelLoader<>(loaderSupplier, closeCallback);
    }

    /**
     * Creates a new wrapped loader for this thread if one doesn't already exist and then passes the object to load to
     * the wrapped loader for the current thread.
     * @param objectToLoad The object to be loaded.
     */
    @Override
    public void load(T objectToLoad)  {
        if (isClosed) {
            IllegalStateException e = new IllegalStateException("ParallelLoader is closed and cannot receive more load requests.");
            logger.error("Error inside ParallelLoader: ", e);
            throw e;
        }

        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ParallelLoader.load")) {
            dataWasLoaded = true;
            try {
                Loader<T> loader = getLoaderForCurrentThread();
                loader.load(objectToLoad);
            } catch (UnrecoverableStreamFailureException e) {
                throw e;
            } catch (Exception e) {
                logger.warn("Exception thrown loading object: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Prepares this loader to begin loading objects
     * @param parentMetrics An EtlMetrics object to attach any child threads created by load() to
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
    }

    /**
     * First, closes all the wrapped loaders that were constructed during load operations. If a closeCallBack method
     * has been registered then it will be invoked.
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        try (EtlProfilingScope ignored = new EtlProfilingScope(parentMetrics, "ParallelLoader.close")) {
            activeLoaders.forEach(loader -> {
                try {
                    loader.close();
                } catch (UnrecoverableStreamFailureException e) {
                    throw e;
                } catch (Exception ignored2) {
                    // No-op
                }
            });

            if (closeCallback != null) {
                try {
                    closeCallback.accept(dataWasLoaded, parentMetrics);
                } catch (UnrecoverableStreamFailureException e) {
                    throw e;
                } catch (Exception e) {
                    logger.error("Exception thrown while executing the closeCallback: ", e);
                    throw new RuntimeException(e);
                }
            }
            this.isClosed = true;
        }
    }

    private Loader<T> getLoaderForCurrentThread() {
        Loader<T> loader = loaderThreadLocal.get();

        if (loader == null) {
            loader = loaderSupplier.get();
            loader.open(parentMetrics);
            loaderThreadLocal.set(loader);
            activeLoaders.add(loader);
        }

        return loader;
    }
}
