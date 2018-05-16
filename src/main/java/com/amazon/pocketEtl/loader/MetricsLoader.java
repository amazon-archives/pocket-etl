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

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.EtlProfilingScope;
import com.amazon.pocketEtl.Loader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Loader implementation that logs metric counters.
 *
 * The implementation of this loader converts the DTO object to map of property name and property value and filters the
 * properties of type Integer. It then emits counter for all the filtered property name as metric name, value as counter
 * value.
 *
 * Since this loader uses Jackson for converting DTO to Map, it is recommended to use @JsonProperty if you want a different
 * name to be used for metric instead of the property name.
 *
 * Example usage:
 * MetricsLoader.of("sampleMetricName")
 *
 * @param <T> The type of objects being loaded.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class MetricsLoader<T> implements Loader<T> {
    public static final String DEFAULT_METRIC_NAME = "MetricsLoader.";
    private final ObjectMapper mapper = new ObjectMapper();
    private final String metricName;

    private EtlMetrics parentMetrics;

    /**
     * Constructs a new MetricsLoader object.
     *
     * @param metricName This name will be prepended to the name of the property. Example, for metric name "foo." and
     *                   integer property name "bar" with value 1 will emit counter: foo.bar = 1.
     *                   NOTE: Metric name must explicitly contain period(.) at the end.
     * @param <T> The type of objects being loaded.
     * @return A constructed MetricsLoader object.
     */
    public static <T> MetricsLoader<T> of(@Nonnull String metricName) {
        return new MetricsLoader<>(metricName);
    }

    /**
     * Constructs a new MetricsLoader object with default metric name.
     *
     * @param <T> The type of objects being loaded.
     * @return A constructed MetricsLoader object.
     */
    public static <T> MetricsLoader<T> of() {
        return new MetricsLoader<>(DEFAULT_METRIC_NAME);
    }

    /**
     * Converts the next object to map and filters all integer property name and value pair and emits counter with property
     * name as metric name and value as counter value.
     *
     * @param objectToLoad The object to be loaded.
     */
    @Override
    public void load(T objectToLoad) {
        try (EtlProfilingScope scope = new EtlProfilingScope(parentMetrics, "MetricsLoader.load")) {
            Map<String, Object> dtoPropertyNameValueMap = mapper.convertValue(objectToLoad, new TypeReference<Map<String, Object>>() {});

            dtoPropertyNameValueMap.forEach((key, value) -> {
                if (value instanceof Integer) {
                    scope.addCounter(metricName + key, (int) value);
                } else if (value instanceof Short) {
                    scope.addCounter(metricName + key, (short) value);
                } else if (value instanceof Byte) {
                    scope.addCounter(metricName + key, (byte) value);
                }
            });
        }
    }

    /**
     * Prepares the loader to start accepting objects to load.
     *
     * @param parentMetrics An EtlMetrics object to attach any child threads created by load() to
     */
    @Override
    public void open(@Nullable EtlMetrics parentMetrics) {
        this.parentMetrics = parentMetrics;
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
