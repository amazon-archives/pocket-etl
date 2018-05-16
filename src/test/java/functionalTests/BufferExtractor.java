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
import com.amazon.pocketEtl.Extractor;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

class BufferExtractor<T> implements Extractor<T> {

    private final Iterator<T> bufferIterator;

    BufferExtractor(List<T> buffer) {
        bufferIterator = buffer.iterator();
    }

    @Override
    public Optional<T> next() {
        if(bufferIterator.hasNext()) {
            return Optional.of(bufferIterator.next());
        }

        return Optional.empty();
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        //no-op
    }
}
