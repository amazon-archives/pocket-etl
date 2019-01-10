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

package com.amazon.pocketEtl.extractor;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.function.Function;

/**
 * A handy mapper function for mapping any json having one json object to a DTO of given type. Powered by Jackson, so
 * you can use Jackson annotations on your DTO class.
 *
 * Usage:
 * JSONStringMapper.of(MyDTO.class);
 *
 * Example valid JSON input:
 * {
 *   "keyOne": "valueOne",
 *   "keyTwo": "valueTwo"
 * }
 *
 * NOTE: Currently this mapper is able to map string dates in json to joda DateTime which are in ISO format.
 * For example, "2017-08-15T12:00:00Z"
 * This is an open bug with jackson API. This needs to be handled in future by adding custom joda datetime
 * serializable or wait for jackson API to fix this.
 */
public class JSONStringMapper<T> implements Function<String, T> {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            // Mapping java bean case conventions can be problematic when the second character is capitalized
            // (eg: getAString()). We are solving this problem by making case not matter.
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .registerModules(new JodaModule(), new Jdk8Module(), new JavaTimeModule());

    private final ObjectReader objectReader;

    public static <T> JSONStringMapper<T> of(Class<T> mapToClass) {
        return new JSONStringMapper<>(mapToClass);
    }

    private JSONStringMapper(Class<T> mapToClass) {
        objectReader = objectMapper.readerFor(mapToClass);
    }

    /**
     * Parse the json string and creates an object of dtoClass with field values populated from parsed json field values.
     * @param json json string to be mapped.
     * @return object of dtoClass type having values populated from json string.
     */
    @Override
    public T apply(String json) {
        if (json == null) {
            return null;
        }

        try {
            return objectReader.readValue(json);
        } catch (UnrecognizedPropertyException e) {
            throw new RuntimeException("Unable to find property in mapToClass. " +
                    "Check for typos or make sure dtoClass has all the fields that are in json: ", e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Make sure the json is valid and contains only one object: ", e);
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse JSON string: ", e);
        }
    }
}