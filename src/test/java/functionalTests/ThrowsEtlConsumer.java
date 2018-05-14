/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
