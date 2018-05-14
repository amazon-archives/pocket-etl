/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.producer.EtlProducer;

/**
 * Internal functional interface for a method that runs an ETL job. Used for test injection.
 */
@FunctionalInterface
interface EtlRunner {
    void run(EtlProducer producer, EtlMetrics parentMetrics) throws Exception;
}
