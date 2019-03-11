/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazon.pocketEtl.EtlStream;
import com.amazon.pocketEtl.Extractor;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.Transformer;
import com.amazon.pocketEtl.exception.UnrecoverableStreamFailureException;
import com.amazon.pocketEtl.extractor.IterableExtractor;

@RunWith(MockitoJUnitRunner.class)
public class StreamFailureTest {
    @Mock
    private Extractor<TestDTO> mockExtractor;

    @Mock
    private Transformer<TestDTO, TestDTO> mockTransformer;

    @Mock
    private Loader<TestDTO> mockLoader;

    @Test
    public void noFailures() throws Exception {
        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void singleThreadedExtractorUnrecoverableFailure() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenThrow(new UnrecoverableStreamFailureException("serious"))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());

        EtlStream.extract(mockExtractor)
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void singleThreadedExtractorUnrecoverableFailureOnClose() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());
        doThrow(new UnrecoverableStreamFailureException("serious")).when(mockExtractor).close();

        EtlStream.extract(mockExtractor)
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void multiThreadedExtractorUnrecoverableFailure() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenThrow(new UnrecoverableStreamFailureException("serious"))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());

        EtlStream.extract(mockExtractor,
                          IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test
    public void multiThreadedExtractorRecoverableFailure() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenThrow(new RuntimeException("not so serious"))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());

        EtlStream.extract(mockExtractor,
                          IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void multiThreadedExtractorUnrecoverableFailureOnClose() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());
        doThrow(new UnrecoverableStreamFailureException("serious")).when(mockExtractor).close();

        EtlStream.extract(mockExtractor,
                          IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test
    public void singleThreadedExtractorRecoverableFailure() throws Exception {
        when(mockExtractor.next()).thenReturn(Optional.of(TestDTO.ONE))
                                  .thenThrow(new RuntimeException("not so serious"))
                                  .thenReturn(Optional.of(TestDTO.TWO))
                                  .thenReturn(Optional.empty());

        EtlStream.extract(mockExtractor)
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void transformerUnrecoverableFailure() throws Exception {
        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, obj -> { throw new UnrecoverableStreamFailureException("serious"); })
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void transformerUnrecoverableFailureOnClose() throws Exception {
        when(mockTransformer.transform(any())).thenReturn(Collections.singletonList(TestDTO.ONE));
        doThrow(new UnrecoverableStreamFailureException("serious")).when(mockTransformer).close();

        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, mockTransformer)
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test
    public void transformerRecoverableFailure() throws Exception {
        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, obj -> { throw new RuntimeException("not so serious"); })
                 .load(TestDTO.class, obj -> {})
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void loaderUnrecoverableFailure() throws Exception {
        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> { throw new UnrecoverableStreamFailureException("serious"); })
                 .run();
    }

    @Test(expected = UnrecoverableStreamFailureException.class)
    public void loaderUnrecoverableFailureOnClose() throws Exception {
        doThrow(new UnrecoverableStreamFailureException("serious")).when(mockLoader).close();

        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, mockLoader)
                 .run();
    }

    @Test
    public void loaderRecoverableFailure() throws Exception {
        EtlStream.extract(IterableExtractor.of(ImmutableList.of(TestDTO.ONE, TestDTO.TWO, TestDTO.THREE)))
                 .transform(TestDTO.class, Collections::singletonList)
                 .load(TestDTO.class, obj -> { throw new RuntimeException("not so serious"); })
                 .run();
    }
}
