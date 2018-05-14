/*
 * Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package functionalTests;

import com.amazon.pocketEtl.EtlStream;
import com.amazon.pocketEtl.extractor.IterableExtractor;
import com.amazon.pocketEtl.transformer.MapTransformer;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.amazon.pocketEtl.EtlConsumerStage.transform;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class SimpleFluentFunctionalTest {
    @Test
    public void testSimpleFluentInterface() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"), new TestDTO("THREE"));
        List<TestDTO> outputData = new ArrayList<>();

        EtlStream.extract(IterableExtractor.of(inputData))
                .then(transform(TestDTO.class,
                        MapTransformer.of(obj -> new TestDTO(obj.getValue().toLowerCase()))).withThreads(5))
                .load(TestDTO.class, outputData::add)
                .run();

        assertThat(outputData, containsInAnyOrder(new TestDTO("one"), new TestDTO("two"), new TestDTO("three")));
    }
}
