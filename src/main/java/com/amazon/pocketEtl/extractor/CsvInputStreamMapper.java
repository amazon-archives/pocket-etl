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

package com.amazon.pocketEtl.extractor;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Implementation of InputStreamMapper that takes an InputStream with CSV formatted data in it and creates an
 * iterator around that data.
 *
 * Powered by Jackson's CsvMapper class, so for precise behavioral information you can refer to their documentation.
 * @param <T> The type of object the CSV data is being mapped to.
 */
@SuppressWarnings("WeakerAccess")
public class CsvInputStreamMapper<T> implements InputStreamMapper<T> {
    private static final CsvMapper mapper = new CsvMapper();

    private final Class<T> objectClass;
    private Character columnSeparator = null;

    /**
     * Convert an InputStream containing CSV data to an iterator. Used by InputStreamExtractor.
     * <p>
     * Example usage:
     * CsvInputStreamMapper.fromCSV(My.class)
     *
     * @param objectClassToMapTo Class definition to map the CSV data into
     * @param <T>                Type of object being iterated over.
     * @return A function that converts an inputStream to an iterator.
     */
    public static <T> CsvInputStreamMapper<T> of(Class<T> objectClassToMapTo) {
        return new CsvInputStreamMapper<>(objectClassToMapTo);
    }

    /**
     * Change the character that is used to delimit the columns in the data on the InputStream. By default this value
     * is a comma.
     * @param columnSeparator The character to treat as a column delimiter.
     * @return A copy of this object with this property changed.
     */
    public CsvInputStreamMapper<T> withColumnSeparator(Character columnSeparator) {
        this.columnSeparator = columnSeparator;
        return this;
    }

    /**
     * Converts an inputStream into an iterator of objects using Jackson CSV mapper.
     * @param inputStream inputStream in CSV format.
     * @return An iterator based on the inputStream.
     */
    @Override
    public Iterator<T> apply(InputStream inputStream) {
        try {
            CsvSchema csvSchema = mapper.schemaFor(objectClass);

            if (columnSeparator != null) {
                csvSchema = csvSchema.withColumnSeparator(columnSeparator);
            }
            
            ObjectReader reader = mapper.readerFor(objectClass).withFeatures(CsvParser.Feature.FAIL_ON_MISSING_COLUMNS)
                    .with(csvSchema);

            return reader.readValues(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CsvInputStreamMapper(
            Class<T> objectClass
    ) {
        this.objectClass = objectClass;
    }
}
