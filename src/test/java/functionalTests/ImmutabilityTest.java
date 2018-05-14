/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package functionalTests;

import com.amazon.pocketEtl.EtlStream;
import com.amazon.pocketEtl.extractor.IterableExtractor;
import com.amazon.pocketEtl.transformer.MapTransformer;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ImmutabilityTest {
    @Test
    public void testStreamImmutability() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"), new TestDTO("THREE"));
        List<TestDTO> outputData = new ArrayList<>();
        EtlStream baseStream = EtlStream.extract(IterableExtractor.of(inputData));

        baseStream.transform(TestDTO.class, MapTransformer.of(obj -> new TestDTO(obj.getValue().toLowerCase())));
        baseStream.load(TestDTO.class, outputData::add)
                .run();

        assertThat(outputData, equalTo(inputData));
    }

    @Test
    public void testStreamReuse() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"));
        List<TestDTO> outputData = new ArrayList<>();
        EtlStream baseStream = EtlStream.extract(IterableExtractor.of(inputData));

        baseStream.transform(TestDTO.class, MapTransformer.of(obj -> new TestDTO(obj.getValue().toLowerCase())))
                .load(TestDTO.class, outputData::add)
                .run();

        baseStream.load(TestDTO.class, outputData::add)
                .run();

        assertThat(outputData, equalTo(ImmutableList.of(new TestDTO("one"), new TestDTO("two"), new TestDTO("ONE"),
                new TestDTO("TWO"))));
    }

    @Test
    public void testCombineImmutability() throws Exception {
        List<TestDTO> inputData1 = ImmutableList.of(new TestDTO("ONE"));
        List<TestDTO> inputData2 = ImmutableList.of(new TestDTO("TWO"));
        List<TestDTO> outputData = new ArrayList<>();

        EtlStream baseStream1 = EtlStream.extract(IterableExtractor.of(inputData1));
        EtlStream baseStream2 = EtlStream.extract(IterableExtractor.of(inputData2));

        EtlStream combinedStream = EtlStream.combine(baseStream1, baseStream2);

        baseStream1.transform(TestDTO.class, MapTransformer.of(obj -> new TestDTO(obj.getValue().toLowerCase())))
                .load(TestDTO.class, outputData::add)
                .run();

        combinedStream.load(TestDTO.class, outputData::add)
                .run();

        assertThat(outputData, equalTo(ImmutableList.of(new TestDTO("one"), new TestDTO("ONE"), new TestDTO("TWO"))));
    }
}
