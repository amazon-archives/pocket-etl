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

package com.amazon.pocketEtl.lookup;

import com.amazon.pocketEtl.EtlTestBase;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class CachingLoaderLookupTest extends EtlTestBase {

    private static final TestDTO SAMPLE_KEY_ONE = new TestDTO("sampleKeyOne", 1);
    private static final TestDTO SAMPLE_KEY_TWO = new TestDTO("sampleKeyTwo", 2);

    @Mock
    private Semaphore mockSemaphore;

    private CachingLoaderLookup<TestDTO> cachingLoaderLookup;
    private CachingLoaderLookup<TestDTO> comparatorLookup;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestDTO implements Comparable<TestDTO> {
        String name;
        int age;

        @Override
        public int compareTo(@Nonnull TestDTO testDTO) {
            return name.equals(testDTO.name) && age == testDTO.age ? 0 : 1;
        }
    }

    @Before
    public void initializeCachingLoaderLookup() {
        cachingLoaderLookup = new CachingLoaderLookup<>(i -> mockSemaphore);
        comparatorLookup = new CachingLoaderLookup<>(i -> mockSemaphore, Comparator.comparing(TestDTO::getName));
    }

    @Test
    public void getReturnsValueForTheKeyFromCache() throws Exception {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        cachingLoaderLookup.load(SAMPLE_KEY_ONE);
        cachingLoaderLookup.close();
        Optional<TestDTO> value = cachingLoaderLookup.get(SAMPLE_KEY_ONE);

        assertThat(value, equalTo(Optional.of(SAMPLE_KEY_ONE)));
    }

    @Test
    public void getReturnsEmptyForTheKeyNotInCache() throws Exception {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        cachingLoaderLookup.load(SAMPLE_KEY_TWO);
        cachingLoaderLookup.close();
        Optional<TestDTO> value = cachingLoaderLookup.get(SAMPLE_KEY_ONE);

        assertThat(value, equalTo(Optional.empty()));
    }

    @Test
    public void verifyIfOpenOnlyAcquiresLockAndCloseReleasesLock() throws Exception {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        verify(mockSemaphore, times(1)).acquireUninterruptibly();

        cachingLoaderLookup.close();
        verify(mockSemaphore, times(1)).release();
    }

    @Test(expected = IllegalStateException.class)
    public void closeThrowsInvalidStateExceptionIfCalledBeforeOpen() throws Exception {
        cachingLoaderLookup.close();
    }

    @Test(expected = IllegalStateException.class)
    public void callingOpenTwiceThrowsIllegalStateException() throws Exception {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
    }

    @Test(expected = IllegalStateException.class)
    public void callingOpenThenCloseThenOpenThrowsIllegalStateException() throws Exception {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        cachingLoaderLookup.close();
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
    }

    @Test(expected = IllegalStateException.class)
    public void callingCloseTwiceThrowsIllegalStateException() throws Exception {
        cachingLoaderLookup.close();
        cachingLoaderLookup.close();
    }

    @Test(expected = IllegalStateException.class)
    public void callingGetBeforeOpenThrowsIllegalStateException() throws Exception {
        cachingLoaderLookup.get(SAMPLE_KEY_ONE);
    }

    @Test
    public void callingGetAfterOpenBeforeCloseDoesNotThrowException() {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        cachingLoaderLookup.get(SAMPLE_KEY_ONE);
    }

    @Test
    public void getAcquiresAndThenReleasesLock() {
        cachingLoaderLookup.open(etlProfilingScope.getMetrics());
        reset(mockSemaphore);

        cachingLoaderLookup.get(SAMPLE_KEY_ONE);

        verify(mockSemaphore, times(1)).acquireUninterruptibly();
        verify(mockSemaphore, times(1)).release();
        verifyNoMoreInteractions(mockSemaphore);
    }

    @Test(expected = IllegalStateException.class)
    public void consumeBeforeOpenThrowsIllegalStateException() throws Exception {
        cachingLoaderLookup.load(SAMPLE_KEY_ONE);
    }

    @Test
    public void getReturnsValueBasedOnComparatorLookup() throws Exception {
        TestDTO dto = new TestDTO("Foo", 1);

        comparatorLookup.open(etlProfilingScope.getMetrics());
        comparatorLookup.load(new TestDTO("Foo", 2));
        comparatorLookup.close();
        Optional<TestDTO> value = comparatorLookup.get(dto);

        assertThat(value, equalTo(Optional.of(dto)));
    }
}
