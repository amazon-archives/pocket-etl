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

import com.amazon.pocketEtl.EtlStream;
import com.amazon.pocketEtl.Extractor;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class SimpleEtlFunctionalTest {
    private final static List<TestDTO> INPUT_LIST_1 = ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE);
    private final static List<TestDTO> INPUT_LIST_2 = ImmutableList.of(TestDTO.FOUR, TestDTO.FIVE, TestDTO.SIX);
    private final static List<TestDTO> INPUT_LIST_3 = ImmutableList.of(TestDTO.SEVEN, TestDTO.EIGHT, TestDTO.NINE);

    private final static TestDTO[] EXPECTED_RESULT = {TestDTO.ONE, TestDTO.TWO, TestDTO.THREE, TestDTO.FOUR, TestDTO.FIVE,
            TestDTO.SIX, TestDTO.SEVEN, TestDTO.EIGHT, TestDTO.NINE};


    private BufferLoader<TestDTO> resultBufferLoader = new BufferLoader<>();
    private ThrowsEtlConsumer errorConsumer = new ThrowsEtlConsumer();

    private void runSimpleTest() throws Exception {

        Extractor[] extractors = {
                new BufferExtractor<>(INPUT_LIST_1),
                new BufferExtractor<>(INPUT_LIST_2),
                new BufferExtractor<>(INPUT_LIST_3)
        };

        EtlStream.extract(extractors)
                .load(TestDTO.class, resultBufferLoader)
                .run();

        assertThat(errorConsumer.isExceptionWasThrown(), is(false));
    }

    @Test
    public void testSimpleEtl() throws Exception {
        runSimpleTest();

        assertThat(resultBufferLoader.getBuffer(), containsInAnyOrder(EXPECTED_RESULT));
    }
}
