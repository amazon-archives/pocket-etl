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

package com.amazon.pocketEtl.core;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Generic data object for the ETL stream. Able to extract views on the map by marshalling key/values into a provided
 * Java Bean class template unless the template matches the object type. The reverse functionality is also provided to
 * map the values from a Java Bean object back into the map, overwriting existing values and preserving any other
 * values that were already in the map. This allows 'attribute tunnelling' where a step in the ETL may only be
 * interested in taking a view on certain attributes before passing the object to the next step, but any attributes
 * already in the data stream will still be passed along to the subsequent steps.
 *
 * If a get is called using the same Bean class that was used to create the class, there is an optimization where the
 * EtlStreamObject will short-circuit introspection and simply return the original object it was created with.
 */
public class EtlStreamObject {
    private final static ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModules(new JodaModule(), new Jdk8Module(), new JavaTimeModule());

    private Map<Object, Object> streamDataMap = null;
    private Object cachedObject;

    public static EtlStreamObject of(Object object) {
        return new EtlStreamObject(object);
    }

    private EtlStreamObject(Object cachedObject) {
        this.cachedObject = cachedObject;
    }

    /**
     * Creates a projection of the data on the stream by instantiating a bean from a provided class and setting its
     * attributes to the values stored on the stream. Will re-use an existing object if one is available.
     * @param dtoClass Class object to use to create the returned object.
     * @param <T> The type of the class object that will be returned.
     * @return A newly created instance of the java bean with the values filled in from the map.
     */
    @SuppressWarnings("unchecked")
    // Explicit check is there but the compiler does not seem to correctly infer that dtoClass is the same class as T
    public <T> T get(Class<T> dtoClass) {
        if (streamDataMap == null && dtoClass.isAssignableFrom(cachedObject.getClass())) {
            return (T)cachedObject;
        }

        initializeStreamDataMap();
        return objectMapper.convertValue(streamDataMap, dtoClass);
    }

    /**
     * Creates an independent copy of this object. All the attributes will be copied onto the new stream. Used by the
     * transformation consumer to fan out the stream. If values of attributes are references to objects, those
     * references will be duplicated, not cloned. This could have unexpected or undesired behavior if those were
     * references to objects that were mutable by design such as containers, atomic references or iterators.
     * @return an independent copy of this object
     */
    public EtlStreamObject createCopy() {
        EtlStreamObject newObject = new EtlStreamObject(cachedObject);
        initializeStreamDataMap();

        Map<Object, Object> newStreamDataMap = new HashMap<>();
        deepMergeMaps(streamDataMap, newStreamDataMap);
        newObject.streamDataMap = newStreamDataMap;

        return newObject;
    }

    /**
     * Fluent method to add the values set on a java bean into the map, overwriting any existing values with the same
     * keys but leaving other keys intact. If the object already on the stream is the same class as the object being
     * merged into the stream, the object will just be replaced as an optimization.
     * @param dto A java bean object to copy the values into the map from.
     * @return A copy of itself.
     */
    public EtlStreamObject with(Object dto) {
        if (streamDataMap == null && cachedObject.getClass().equals(dto.getClass())) {
            cachedObject = dto;
            return this;
        }

        initializeStreamDataMap();
        set(dto);
        return this;
    }

    private void initializeStreamDataMap() {
        if (streamDataMap == null) {
            streamDataMap = new HashMap<>();
            set(cachedObject);
        }
    }

    private void set(Object dto) {
        Map<Object, Object> updateMap = objectMapper.convertValue(dto, new TypeReference<Map<String, Object>>() {});
        deepMergeMaps(updateMap, streamDataMap);
    }

    @SuppressWarnings("unchecked")
    private static void deepMergeMaps(Map<Object, Object> fromMap, Map<Object, Object> toMap) {
        fromMap.forEach((key, value) -> {
            if (value instanceof Map) {
                Object targetObject = toMap.get(key);

                if (targetObject instanceof Map) {
                    deepMergeMaps((Map)value, (Map)targetObject);
                    return;
                }
            }
            toMap.put(key, value);
        });
    }
}