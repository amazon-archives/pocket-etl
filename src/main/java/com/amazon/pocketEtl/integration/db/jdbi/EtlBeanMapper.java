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

package com.amazon.pocketEtl.integration.db.jdbi;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Implementation of JDBI ResultSetMapper that will map any basic DTO and figure out how to extract the values from
 * the ResultSet based on reflection of the DTO object. Heavily based on the default bean mapper provided by JDBI with
 * added support for Joda DateTime.
 *
 * Also, once the column extracted from the resultset does not map to any property in DTO and DTO has a property named
 * 'otherInformation' of type Map, then it adds the column name and value in a LinkedHashMap and adds it to the DTO.
 *
 * @param <T> Type of DTO being mapped into
 */
class EtlBeanMapper<T> implements ResultSetMapper<T> {
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final Class<T> type;
    private final Map<String, PropertyDescriptor> properties = new HashMap<>();
    private final BiConsumer<T, Map.Entry<String, String>> secondaryMapper;

    EtlBeanMapper(Class<T> type, @Nullable BiConsumer<T, Map.Entry<String, String>> secondaryMapper)
    {
        this.type = type;
        this.secondaryMapper = secondaryMapper;

        try {
            BeanInfo info = Introspector.getBeanInfo(type);

            for (PropertyDescriptor descriptor : info.getPropertyDescriptors()) {
                properties.put(descriptor.getName().toLowerCase(), descriptor);
            }
        }
        catch (IntrospectionException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public T map(int row, ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        T bean;
        try {
            bean = type.newInstance();
        }
        catch (Exception e) {
            throw new IllegalArgumentException(String.format("A bean, %s, was mapped " +
                    "which was not instantiable", type.getName()), e);
        }

        ResultSetMetaData metadata = rs.getMetaData();

        for (int i = 1; i <= metadata.getColumnCount(); ++i) {
            String name = metadata.getColumnLabel(i).toLowerCase();

            PropertyDescriptor descriptor = properties.get(name);

            if (descriptor != null) {
                Class type = descriptor.getPropertyType();
                Object value = getValueForTypeFromResultSet(i, rs, type);
                setBeanPropertyValue(bean, descriptor, name, value);
            } else if (secondaryMapper != null) {
                String value;
                int columnType = metadata.getColumnType(i);

                if(columnType == Types.DATE || columnType == Types.TIMESTAMP) {
                    DateTime dateTime = (DateTime) getValueForTypeFromResultSet(i, rs, DateTime.class);
                    value = dateTime.toString(DateTimeFormat.forPattern(DATE_TIME_FORMAT));
                } else {
                    value = (String)getValueForTypeFromResultSet(i, rs, String.class);
                }

                secondaryMapper.accept(bean, new SimpleImmutableEntry(metadata.getColumnLabel(i), value));
            }
        }

        return bean;
    }

    @SuppressWarnings("unchecked")
    private Object getValueForTypeFromResultSet(int columnIndex, ResultSet rs, Class type) throws SQLException {
        Object value;

        if (type.isAssignableFrom(Boolean.class) || type.isAssignableFrom(boolean.class)) {
            value = rs.getBoolean(columnIndex);
        }
        else if (type.isAssignableFrom(Byte.class) || type.isAssignableFrom(byte.class)) {
            value = rs.getByte(columnIndex);
        }
        else if (type.isAssignableFrom(Short.class) || type.isAssignableFrom(short.class)) {
            value = rs.getShort(columnIndex);
        }
        else if (type.isAssignableFrom(Integer.class) || type.isAssignableFrom(int.class)) {
            value = rs.getInt(columnIndex);
        }
        else if (type.isAssignableFrom(Long.class) || type.isAssignableFrom(long.class)) {
            value = rs.getLong(columnIndex);
        }
        else if (type.isAssignableFrom(Float.class) || type.isAssignableFrom(float.class)) {
            value = rs.getFloat(columnIndex);
        }
        else if (type.isAssignableFrom(Double.class) || type.isAssignableFrom(double.class)) {
            value = rs.getDouble(columnIndex);
        }
        else if (type.isAssignableFrom(BigDecimal.class)) {
            value = rs.getBigDecimal(columnIndex);
        }
        else if (type.isAssignableFrom(Timestamp.class)) {
            value = rs.getTimestamp(columnIndex);
        }
        else if (type.isAssignableFrom(DateTime.class)) {
            Timestamp ts = rs.getTimestamp(columnIndex);

            if (ts != null) {
                value = new DateTime(ts.getTime());
            }
            else {
                value = null;
            }
        }
        else if (type.isAssignableFrom(Time.class)) {
            value = rs.getTime(columnIndex);
        }
        else if (type.isAssignableFrom(Date.class)) {
            value = rs.getDate(columnIndex);
        }
        else if (type.isAssignableFrom(String.class)) {
            value = rs.getString(columnIndex);
        }
        else if (type.isEnum()) {
            value = Enum.valueOf(type, rs.getString(columnIndex));
        }
        else {
            value = rs.getObject(columnIndex);
        }

        if (rs.wasNull() && !type.isPrimitive()) {
            value = null;
        }

        return value;
    }

    private void setBeanPropertyValue(T bean, PropertyDescriptor descriptor, String name, Object value) {
        try
        {
            descriptor.getWriteMethod().invoke(bean, value);
        }
        catch (IllegalAccessException e) {
            throw new IllegalArgumentException(String.format("Unable to access setter for " +
                    "property, %s", name), e);
        }
        catch (InvocationTargetException e) {
            throw new IllegalArgumentException(String.format("Invocation target exception trying to " +
                    "invoker setter for the %s property", name), e);
        }
        catch (NullPointerException e) {
            throw new IllegalArgumentException(String.format("No appropriate method to " +
                    "write property %s", name), e);
        }
    }
}
