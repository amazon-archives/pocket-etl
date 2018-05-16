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

package com.amazon.pocketEtl.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import lombok.EqualsAndHashCode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic data object for the ETL stream, wraps a LinkedHashMap. Able to extract views on the map by marshalling key/values
 * into a provided Java Bean class template. The reverse functionality is also provided to map the values from a Java
 * Bean object back into the map, overwriting existing values and preserving any other values that were already in the
 * map. This allows 'attribute tunnelling' where a step in the ETL may only be interested in taking a view on certain
 * attributes before passing the object to the next step, but any attributes already in the data stream will still be
 * passed along to the subsequent steps.
 */
@EqualsAndHashCode
public class EtlStreamObject {
    private final static ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(new JodaModule());

    private final Map<String, Object> streamDataMap;

    /**
     * Default constructor, starts with an empty map.
     */
    public EtlStreamObject() {
        streamDataMap = new LinkedHashMap<>();
    }

    /**
     * Used to copy a stream object from another one. Will add all the values from the supplied EtlStreamObject into a
     * newly created one, thus creating an entirely independent copy of the original as long as all of the values in the
     * map are references immutable objects (shallow copy).
     * @param etlStreamObjectToCopyFrom An EtlStreamObject to copy from.
     */
    public EtlStreamObject(EtlStreamObject etlStreamObjectToCopyFrom) {
        streamDataMap = new LinkedHashMap<>(etlStreamObjectToCopyFrom.streamDataMap);
    }

    /**
     * Creates ands populates the fields of java bean to provide a view of the underlying map.
     * @param dtoClass Class object to use to create the returned object.
     * @param <T> The type of the class object that will be returned.
     * @return A newly created instance of the java bean with the values filled in from the map.
     */
    public <T> T get(Class<T> dtoClass) {
        return objectMapper.convertValue(streamDataMap, dtoClass);
    }

    /**
     * Copies the values set on a java bean into the map, overwriting any existing values with the same keys but leaving
     * other keys intact. This method mutates the underlying map. If the mapped data structure is deep (maps within maps)
     * then those maps are assumed to be the result of prior deserialization, thus keyed by Strings, and will be deep
     * merged into the target map to allow tunnelling of deep attributes without being overwritten from higher level
     * objects.
     * @param dto A java bean object to copy the values into the map from.
     */
    public void set(Object dto) {
        Map<String, Object> updateMap = objectMapper.convertValue(dto, new TypeReference<Map<String, Object>>() {});
        deepMergeMaps(updateMap, streamDataMap);
    }

    /**
     * Fluent method to add the values set on a java bean into the map, overwriting any existing values with the same
     * keys but leaving other keys intact. This method mutates the underlying map.
     * @param dto A java bean object to copy the values into the map from.
     * @return A copy of itself.
     */
    public EtlStreamObject with(Object dto) {
        set(dto);
        return this;
    }

    @SuppressWarnings("unchecked")
    private static void deepMergeMaps(Map<String, Object> fromMap, Map<String, Object> toMap) {
        fromMap.forEach((key, value) -> {
            if (value instanceof Map) {
                Object targetObject = toMap.get(key);

                if (targetObject instanceof Map) {
                    deepMergeMaps((Map<String, Object>) value, (Map<String, Object>) targetObject);
                    return;
                }
            }
            toMap.put(key, value);
        });
    }
}
