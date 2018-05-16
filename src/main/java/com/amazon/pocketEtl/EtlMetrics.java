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

package com.amazon.pocketEtl;

import java.io.Closeable;

/**
 * Interface for an object to record counters and timers for metrics and profiling. Clients wishing to get profiling
 * data from their PocketETL jobs should write an implementation for this interface or an adapter to a metrics library
 * of their choice.
 */
public interface EtlMetrics extends Closeable {
    /**
     * Create a child metrics object that remains linked to this one. When the child metrics object is closed, all of its
     * data rolls up into the parent metrics object that created it. A new child metrics object would thus be created
     * every time a new profiling scope was entered.
     * @return A new EtlMetrics object that is a linked child to the current EtlMetrics object.
     */
    EtlMetrics createChildMetrics();

    /**
     * Adds a count to the metrics object. Aggregation strategies can vary depending on implementation.
     * @param keyName The metrics key.
     * @param valueInUnits The value of the count.
     */
    void addCount(String keyName, double valueInUnits);

    /**
     * Adds a time to the metrics object. Aggregation strategies can vary depending on implementation.
     * @param keyName The metrics key.
     * @param valueInMilliSeconds The value of the timer in milliseconds.
     */
    void addTime(String keyName, double valueInMilliSeconds);

    /**
     * Closes the metrics object and if linked to a parent metrics object will roll-up all timers and counts to the
     * parent metrics object.
     */
    @Override
    void close();
}