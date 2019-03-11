/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Exception that can be thrown by any step in a job when an unrecoverable problem has occurred and the stream needs to
 * be aborted at the first opportunity. An example would be losing access to a dependent resource such as a database
 * or filesystem or a problem that is likely to effect an entire class or batch of objects rather than a single
 * object such as a batched flush failing on a loader.
 *
 * If any step in the ETL stream throws this exception then it will immediately propagate through the stream shutting
 * down every upstream step and eventually failing the entire job.
 */
public class UnrecoverableStreamFailureException extends RuntimeException {
    /**
     * Standard constructor.
     * @param s Exception message giving more details of the problem.
     */
    public UnrecoverableStreamFailureException(String s) {
        super(s);
    }

    /**
     * Standard constructor.
     * @param cause Exception that caused this failure.
     */
    public UnrecoverableStreamFailureException(Throwable cause) {
        super(cause);
    }

    /**
     * Standard constructor.
     * @param message Exception message giving more details of the problem.
     * @param cause Exception that caused this failure.
     */
    public UnrecoverableStreamFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}