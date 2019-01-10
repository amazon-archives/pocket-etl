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

package com.amazon.pocketEtl.exception;

/**
 * Exception thrown when a dependency is missing. For example, null Connection is returned by database dataSource.
 */
public class DependencyException extends RuntimeException {
    /**
     * Standard constructor.
     * @param s Exception message giving more details of the problem.
     */
    public DependencyException(String s) {
        super(s);
    }
}
