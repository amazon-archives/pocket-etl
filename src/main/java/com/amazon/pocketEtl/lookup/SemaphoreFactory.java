/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.pocketEtl.lookup;

import java.util.concurrent.Semaphore;

/**
 * Simple factory interface for creating Semaphore objects.
 */
public interface SemaphoreFactory {
    /**
     * Gets a new semaphore object.
     * @param numberOfPermits Number of permits to initialize the Semaphore with.
     * @return A new Semaphore object.
     */
    Semaphore get(int numberOfPermits);
}
