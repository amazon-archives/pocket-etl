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

package com.amazon.pocketEtl.lookup;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.Loader;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of Lookup that builds a cached data-set from a stream of data fed to it as a loader. To accomplish
 * this, this class acts as both a lookup and a loader and implements both interfaces. The lookup implementation uses
 * the same type as the key and value to match the pattern of the loader.
 *
 * This lookup will block on any calls to get() until the data stream populating the cache has been closed.
 *
 * @param <T> The type of object that is being loaded/looked-up. If you do not use a comparator, this class
 *          must implement Comparable.
 */
public class CachingLoaderLookup<T> implements Lookup<T, T>, Loader<T> {
    private enum State {
        INITIALIZED,
        CACHE_LOADING,
        CACHE_LOADED
    }

    private final Set<T> objectCache;
    private final Semaphore loaderSemaphore;
    private AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);

    /**
     * Standard constructor.
     * @param semaphoreFactory A factory to create new semaphores.
     */
    public CachingLoaderLookup(SemaphoreFactory semaphoreFactory) {
        this(semaphoreFactory, null);
    }

    /**
     * Constructs a CachingConsumerLookup that uses a comparator for its lookup. This is a good
     * solution when using the lookup for classes that do not implement Comparable.
     *
     * @param semaphoreFactory A factory to create new semaphores.
     * @param comparator A comparator used as the basis for the lookup.
     */
    public CachingLoaderLookup(SemaphoreFactory semaphoreFactory, Comparator<T> comparator) {
        loaderSemaphore = semaphoreFactory.get(1);

        if (comparator == null) {
            objectCache = new ConcurrentSkipListSet<>();
        } else {
            objectCache = new ConcurrentSkipListSet<>(comparator);
        }
    }

    /**
     * Load a data object to be cached in the lookup.
     * @param objectToLoad The object to be cached.
     * @throws IllegalStateException If the loader is in a state that is unable to cache more values.
     */
    @Override
    public void load(T objectToLoad) throws IllegalStateException {
        if (state.get() != State.CACHE_LOADING) {
            throw new IllegalStateException("Attempt to consume CachingConsumerLookup that is in invalid state '" +
                    state.get().name() + "'");
        }

        objectCache.add(objectToLoad);
    }

    /**
     * Prepare the loader to begin caching objects.
     * @param parentMetrics An EtlMetrics object that encompasses the whole ETL job.
     */
    @Override
    public void open(EtlMetrics parentMetrics) {
        if (state.get() != State.INITIALIZED) {
            throw new IllegalStateException("Attempt to open CachingConsumerLookup that is in invalid state '" +
                    state.get().name() + "'");
        }

        loaderSemaphore.acquireUninterruptibly();
        state.compareAndSet(State.INITIALIZED, State.CACHE_LOADING);
    }

    /**
     * Finalize the cache, this will enable the lookup to begin retrieving values.
     * @throws Exception If something goes wrong.
     */
    @Override
    public void close() throws Exception {
        if (state.get() != State.CACHE_LOADING) {
            throw new IllegalStateException("Attempt to close CachingConsumerLookup that is in invalid state '" +
                    state.get().name() + "'");
        }

        // Unblocks/enables any threads waiting in get()
        loaderSemaphore.release();

        state.compareAndSet(State.CACHE_LOADING, State.CACHE_LOADED);
    }

    /**
     * Attempts to lookup a value from the cache. If the cache has not yet been fully populated (close() has not been
     * called) then this method will block until it has been finalized. Careless use of this method will cause your
     * job to deadlock, the thread that is calling get() must never block the work being done to populate and finalize
     * the loader fronted cache.
     * @param key The key to search the cache for.
     * @return The key (not the stored value) if it was found or empty if it was not in the cache.
     */
    @Override
    public Optional<T> get(T key) {
        if (state.get() != State.CACHE_LOADING && state.get() != State.CACHE_LOADED) {
            throw new IllegalStateException("Attempt to get CachingConsumerLookup that is in invalid state '" +
                    state.get().name() + "'");
        }

        // If the cache has not been fully loaded yet, use this lock object to block this thread until loading is complete
        if (state.get() == State.CACHE_LOADING) {
            loaderSemaphore.acquireUninterruptibly();
            loaderSemaphore.release();
        }

        return objectCache.contains(key) ? Optional.of(key) : Optional.empty();
    }
}
