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

package functionalTests;

import com.amazon.pocketEtl.EtlMetrics;
import com.amazon.pocketEtl.core.EtlStreamObject;
import com.amazon.pocketEtl.core.consumer.EtlConsumer;
import lombok.AccessLevel;
import lombok.Getter;

public class ThrowsEtlConsumer implements EtlConsumer {
    @Getter(AccessLevel.PUBLIC)
    private boolean exceptionWasThrown = false;

    @Override
    public void consume(EtlStreamObject objectToConsume) throws IllegalStateException {
        exceptionWasThrown = true;
        throw new RuntimeException("ThrowsConsumer consumed object: " + objectToConsume.toString());
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
