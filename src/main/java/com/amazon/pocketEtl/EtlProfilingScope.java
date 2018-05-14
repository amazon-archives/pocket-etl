/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * AutoCloseable wrapper for an EtlMetrics object to support profiling.
 * <p/>
 * <h2>Example usage:<h2/>
 * {@code
 * try ( EtlProfilingScope etlProfilingScope = new EtlProfilingScope("MyLogEntry"); ) {
 *     boolean hadSuccess = false;
 *     try {
 *         // Do stuff here
 *         // for example:
 *         for (int i=0; i<100; i++) {
 *             etlProfilingScope.addCounter("itemsProcessed",1);
 *         }
 *         hadSuccess = true;
 *     } finally {
 *         if (hadSuccess) {
 *             etlProfilingScope.addCounter("success",1);
 *             etlProfilingScope.addCounter("failure",0);
 *         } else {
 *             etlProfilingScope.addCounter("success",0);
 *             etlProfilingScope.addCounter("failure",1);
 *         }
 *     }
 * } // etlProfilingScope is auto-closed here.
 * }
 */
public class EtlProfilingScope implements AutoCloseable {
    private final static Log logger = LogFactory.getLog(EtlProfilingScope.class);
    private final static ThreadLocal<EtlMetrics> rootMetrics = new ThreadLocal<>();
    private final EtlMetrics ourMetrics;
    private final long metricsStart;
    private final boolean isRoot;
    private final String scopeName;
    private boolean closed = false;

    /**
     * Create a new profiling scope with the given scope name.<p/>
     * This creates a child metrics scope and does not touch the passed in Metrics objects.<p/>
     * @param metrics The EtlMetrics object to store timers and counters for the scope.
     * @param scopeName Name of profiling scope.
     */
    public EtlProfilingScope(final EtlMetrics metrics, final String scopeName) {
        this(metrics, scopeName, true);
    }

    private EtlProfilingScope(final EtlMetrics metrics, final String scopeName, final boolean createChild) {
        if (createChild && metrics != null) {
            ourMetrics = metrics.createChildMetrics();
        } else {
            ourMetrics = metrics;
        }

        if (rootMetrics.get() == null) {
            rootMetrics.set(ourMetrics);
            isRoot = true;
        } else {
            isRoot = false;
        }

        this.scopeName = scopeName;
        metricsStart = System.currentTimeMillis();
    }

    /**
     * Return the actual EtlMetrics object. If you are calling EtlProfilingScope.close or using try-with-resource,
     * do NOT call close on this returned object. If you create any child metrics from this object you MUST
     * call close on the child metrics objects.
     */
    public EtlMetrics getMetrics() {
        return ourMetrics;
    }

    /**
     * Add (increment) the counter by value.
     * @param counterName Name of counter
     * @param value A value to adjust the counter by.
     */
    public void addCounter(String counterName, int value) {
        if (closed) {
            logger.error("Scope " + scopeName + " called addCounter after being closed.");
            return;
        }

        if (ourMetrics != null) {
            ourMetrics.addCount(counterName, value);
        }
    }

    /**
     * Close out the scope.
     */
    @Override
    public void close() {
        if (closed) {
            logger.error("Scope " + scopeName + " called close after being closed.");
            return;
        }
        closed = true;

        if (isRoot) {
            rootMetrics.remove();
        }

        if (ourMetrics != null) {
            final long metricsEnd = System.currentTimeMillis();
            ourMetrics.addTime(scopeName, metricsEnd - metricsStart);
            ourMetrics.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (!closed) {
                logger.fatal("Profiler scope " + scopeName + " was created, but never closed.");
                close();
            }
        } finally {
            super.finalize();
        }
    }
}