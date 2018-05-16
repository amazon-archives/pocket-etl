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
import com.amazon.pocketEtl.lookup.CachingLoaderLookup;
import com.amazon.pocketEtl.lookup.Lookup;
import com.amazon.pocketEtl.transformer.FilterTransformer;
import com.amazon.pocketEtl.transformer.filter.ContainsFilter;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.BiPredicate;

import static com.amazon.pocketEtl.EtlConsumerStage.load;
import static com.amazon.pocketEtl.EtlConsumerStage.transform;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class FilterFunctionalTest {
    private final static List<TestDTO> INPUT_LIST_1 = ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE);
    private final static List<TestDTO> INPUT_LIST_2 = ImmutableList.of(TestDTO.FOUR, TestDTO.FIVE, TestDTO.SIX);
    private final static List<TestDTO> INPUT_LIST_3 = ImmutableList.of(TestDTO.SEVEN, TestDTO.EIGHT, TestDTO.NINE);

    private final static List<TestDTO> FILTER_LIST_1 = ImmutableList.of(TestDTO.ONE, TestDTO.FIVE, TestDTO.TEN);
    private final static List<TestDTO> FILTER_LIST_2 = ImmutableList.of(TestDTO.ELEVEN, TestDTO.NINE, TestDTO.ZERO);

    private final static TestDTO[] EXPECTED_RESULT = {TestDTO.ONE, TestDTO.FIVE, TestDTO.NINE};
    private final static TestDTO[] EXPECTED_NEGATIVE_RESULT = {TestDTO.TWO, TestDTO.THREE, TestDTO.FOUR, TestDTO.SIX, TestDTO.SEVEN, TestDTO.EIGHT};

    private BufferLoader<TestDTO> resultBufferLoader = new BufferLoader<>();
    private ThrowsEtlConsumer errorConsumer = new ThrowsEtlConsumer();

    private void runFilterTest(BiPredicate<TestDTO, Lookup<TestDTO, TestDTO>> filter) throws Exception {
        CachingLoaderLookup<TestDTO> cachingLoaderLookup = new CachingLoaderLookup<>(Semaphore::new);
        Transformer<TestDTO, TestDTO> filterTransformer = new FilterTransformer<>(filter, cachingLoaderLookup);

        Extractor[] extractors = {
                new BufferExtractor<>(INPUT_LIST_1),
                new BufferExtractor<>(INPUT_LIST_2),
                new BufferExtractor<>(INPUT_LIST_3)
        };

        EtlStream cacheLoaderStream =
                EtlStream.extract(new BufferExtractor<>(FILTER_LIST_1), new BufferExtractor<>(FILTER_LIST_2))
                .then(load(TestDTO.class, cachingLoaderLookup).withThreads(5));

        EtlStream sourceEtlStream = EtlStream.extract(extractors)
                .then(transform(TestDTO.class, filterTransformer).withThreads(5));

        EtlStream.combine(sourceEtlStream, cacheLoaderStream)
                .load(TestDTO.class, resultBufferLoader)
                .run();

        assertThat(errorConsumer.isExceptionWasThrown(), is(false));
    }

    @Test
    public void testContainsFilter() throws Exception {
        runFilterTest(new ContainsFilter<>());

        assertThat(resultBufferLoader.getBuffer(), containsInAnyOrder(EXPECTED_RESULT));
    }

    @Test
    public void testNotContainsFilter() throws Exception {
        runFilterTest(new ContainsFilter<TestDTO>().negate());

        assertThat(resultBufferLoader.getBuffer(), containsInAnyOrder(EXPECTED_NEGATIVE_RESULT));
    }
}
