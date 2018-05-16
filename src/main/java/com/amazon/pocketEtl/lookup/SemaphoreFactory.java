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
