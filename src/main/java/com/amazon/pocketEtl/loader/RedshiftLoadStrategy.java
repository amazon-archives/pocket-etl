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

package com.amazon.pocketEtl.loader;

/**
 * Enumeration of behavior of bulk redshift load from S3.
 */
public enum RedshiftLoadStrategy {
    /**
     * MERGE_INTO_EXISTING_DATA will update existing rows in redshift table and new records will be inserted.
     *
     * For example, If your original redshift table has following records.
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_one </td> <td> column_value_one </td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> column_value_two </td>
     *   </tr>
     * </table>
     * <br>
     *
     * and you are trying to load following data.
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> updated_column_value_two </td>
     *   </tr>
     *   <tr>
     *     <td> key_three </td> <td> column_value_three </td>
     *   </tr>
     * </table>
     * <br>
     *
     * then, after MERGE_INTO_EXISTING_DATA load, your original redshift table will be
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_one </td> <td> column_value_one </td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> updated_column_value_two </td>
     *   </tr>
     *   <tr>
     *     <td> key_three </td> <td> column_value_three </td>
     *   </tr>
     * </table>
     */
    MERGE_INTO_EXISTING_DATA,

    /**
     * CLOBBER_EXISTING_DATA will truncate existing data and insert new data in redshift table.
     *
     * For example, If your original redshift table has following records.
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_one </td> <td> column_value_one </td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> column_value_two </td>
     *   </tr>
     * </table>
     * <br>
     *
     * and you are trying to load following data.
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> updated_column_value_two </td>
     *   </tr>
     *   <tr>
     *     <td> key_three </td> <td> column_value_three </td>
     *   </tr>
     * </table>
     * <br>
     *
     * then, after CLOBBER_EXISTING_DATA load, your original redshift table will become
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> updated_column_value_two </td>
     *   </tr>
     *   <tr>
     *     <td> key_three </td> <td> column_value_three </td>
     *   </tr>
     * </table>
     * <br>
     *
     * NOTE: If you are trying to load empty records to redshift table, then CLOBBER_EXISTING_DATA will truncate
     * existing table.
     *
     * For example, If your original redshift table has following records.
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     *   <tr>
     *     <td> key_one </td> <td> column_value_one </td>
     *   </tr>
     *   <tr>
     *     <td> key_two </td> <td> column_value_two </td>
     *   </tr>
     * </table>
     * <br>
     *
     * and you are trying to load empty data, then, your original table will become
     *
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> primary_key_column </td> <td> column</td>
     *   </tr>
     * </table>
     */
    CLOBBER_EXISTING_DATA
}
