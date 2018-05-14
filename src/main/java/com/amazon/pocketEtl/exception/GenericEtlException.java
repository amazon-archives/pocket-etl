/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.exception;

/**
 * Exception thrown when there is a problem in the underlying service.
 */
public class GenericEtlException extends Exception {
    /**
     * Standard constructor.
     * @param s Exception message giving more details of the problem.
     */
    public GenericEtlException(String s) {
        super(s);
    }
}