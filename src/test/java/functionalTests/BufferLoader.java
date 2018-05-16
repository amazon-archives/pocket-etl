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
import com.amazon.pocketEtl.Loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BufferLoader<T> implements Loader<T> {
    private final ConcurrentLinkedQueue<T> bufferLinkedQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void load(T objectToLoad) {
        bufferLinkedQueue.add(objectToLoad);
    }

    @Override
    public void open(EtlMetrics parentMetrics) {
        //no-op
    }

    @Override
    public void close() throws Exception {
        //no-op
    }

    List<T> getBuffer() {
         return new ArrayList<>(bufferLinkedQueue);
    }
}
