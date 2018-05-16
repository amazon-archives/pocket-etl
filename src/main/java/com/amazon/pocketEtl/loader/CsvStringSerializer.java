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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.joda.JodaModule;

/**
 * A StringSerializer implementation for serializing a DTO to a delimited String with specified columnSeparator.
 * If a headerRow is used, this serializer is not thread-safe.
 *
 * PocketETL developer note: Currently it serializes joda datetime to string in UTC timezone. This behaviour can be
 * changed by setting default timezone using setTimeZone method of CsvMapper class.
 *
 * @param <T> Type of DTO to be deserialized.
 */
@SuppressWarnings("WeakerAccess")
public class CsvStringSerializer<T> implements StringSerializer<T> {
    private static final CsvMapper mapper = new CsvMapper();

    private static final char DEFAULT_COLUMN_SEPARATOR = ',';

    private final Class<T> classToSerialize;
    private final Character columnSeparator;
    private final Boolean writeHeaderRow;
    private ObjectWriter firstRowWriter;
    private ObjectWriter writer;

    private boolean hasWrittenFirstRow = false;

    /**
     * Standard constructor. Uses commas as a default columnSeparator.
     *
     * @param classToSerialize Class of DTO to be deserialized.
     */
    public static <T> CsvStringSerializer<T> of(Class<T> classToSerialize) {
        return new CsvStringSerializer<>(classToSerialize, null, null);
    }

    /**
     * Change the character to use to delimit columns that are being output.
     * @param columnSeparator Delimiter to be used as column separator.
     * @return A new modified CsvStringSerializer.
     */
    public CsvStringSerializer<T> withColumnSeparator(Character columnSeparator) {
        return new CsvStringSerializer<>(classToSerialize, columnSeparator, writeHeaderRow);
    }

    /**
     * Modify the behavior of this serializer to either start writing a header row the first time it is called or
     * stop doing so.
     * @param writeHeaderRow True to writer a header row the first time it is called; or false not to
     * @return A newly initialized copy of the existing object with this behavior changed.
     */
    public CsvStringSerializer<T> withHeaderRow(Boolean writeHeaderRow) {
        return new CsvStringSerializer<>(classToSerialize, columnSeparator, writeHeaderRow);
    }

    /**
     * Method for converting dto object to delimited string without quote character.
     *
     * @param objectToDeserialize Object to be deserialized.
     * @return Returns a delimited String.
     */
    @Override
    public String apply(T objectToDeserialize) {
        try {
            if (!hasWrittenFirstRow) {
                hasWrittenFirstRow = true;
                return firstRowWriter.writeValueAsString(objectToDeserialize);
            }

            return writer.writeValueAsString(objectToDeserialize);
        } catch (JsonProcessingException e){
            throw new RuntimeException(e);
        }
    }

    private CsvStringSerializer(Class<T> classToSerialize, Character columnSeparator, Boolean writeHeaderRow) {
        this.classToSerialize = classToSerialize;
        this.columnSeparator = columnSeparator;
        this.writeHeaderRow = writeHeaderRow;
        createObjectWriter();
    }

    private void createObjectWriter() {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new JodaModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        CsvSchema schema = mapper.schemaFor(classToSerialize).withColumnSeparator(getColumnSeparator()).withoutQuoteChar();
        writer = mapper.writer(schema);

        if (getWriteHeaderRow()) {
            schema = schema.withHeader();
        }

        firstRowWriter = mapper.writer(schema);
    }

    private boolean getWriteHeaderRow() {
        return Boolean.TRUE.equals(writeHeaderRow);
    }

    private char getColumnSeparator() {
        return columnSeparator == null ? DEFAULT_COLUMN_SEPARATOR : columnSeparator;
    }
}
