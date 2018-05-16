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
import com.amazon.pocketEtl.Transformer;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;
import java.util.Locale;

import static com.amazon.pocketEtl.EtlConsumerStage.load;
import static com.amazon.pocketEtl.EtlConsumerStage.transform;
import static com.amazon.pocketEtl.EtlProducerStage.extract;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class TransformerFanoutTest {
    private final static List<TestDTO> INPUT_LIST_1 = ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE);
    private final static List<TestDTO> INPUT_LIST_2 = ImmutableList.of(TestDTO.FOUR, TestDTO.FIVE, TestDTO.SIX);
    private final static List<TestDTO> INPUT_LIST_3 = ImmutableList.of(TestDTO.SEVEN, TestDTO.EIGHT, TestDTO.NINE);

    private final static TestDTO[] EXPECTED_RESULT = {TestDTO.ONE, TestDTO.TWO, TestDTO.THREE, TestDTO.FOUR, TestDTO.FIVE,
            TestDTO.SIX, TestDTO.SEVEN, TestDTO.EIGHT, TestDTO.NINE, new TestDTO("one"), new TestDTO("two"), new TestDTO("three"),
            new TestDTO("four"), new TestDTO("five"), new TestDTO("six"), new TestDTO("seven"), new TestDTO("eight"),
            new TestDTO("nine")};

    private BufferLoader<TestDTO> resultBufferLoader = new BufferLoader<>();
    private ThrowsEtlConsumer errorConsumer = new ThrowsEtlConsumer();

    private Transformer<TestDTO, TestDTO> lowerCaseFanoutTransformer =
            word -> ImmutableList.of(word, new TestDTO(word.getValue().toLowerCase(Locale.ENGLISH)));

    private void runTest() throws Exception {
        Extractor[] extractors = {
                new BufferExtractor<>(INPUT_LIST_1),
                new BufferExtractor<>(INPUT_LIST_2),
                new BufferExtractor<>(INPUT_LIST_3)
        };

        EtlStream.from(extract(extractors))
                .then(transform(TestDTO.class, lowerCaseFanoutTransformer).withThreads(5))
                .then(load(TestDTO.class, resultBufferLoader).withThreads(5))
                .run();

        assertThat(errorConsumer.isExceptionWasThrown(), is(false));
    }

    @Test
    public void testFanoutTransformer() throws Exception {
        runTest();

        assertThat(resultBufferLoader.getBuffer(), containsInAnyOrder(EXPECTED_RESULT));
    }
}
