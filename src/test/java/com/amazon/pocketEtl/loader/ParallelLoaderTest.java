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

package com.amazon.pocketEtl.loader;

import com.amazon.pocketEtl.EtlTestBase;
import com.amazon.pocketEtl.Loader;
import com.amazon.pocketEtl.core.executor.EtlExecutor;
import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ParallelLoaderTest extends EtlTestBase {

    private final static String TEST_STRING = "hello world";
    private static final int NUMBER_OF_LOADERS = 5;
    private static final int EXECUTOR_QUEUE_SIZE = 10;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Supplier<Loader<String>> mockLoaderSupplier;

    @Mock
    private Supplier<Loader<String>> mockMultiThreadedLoaderSupplier;

    @Mock
    private Loader<String> mockLoader;
    
    private ParallelLoader<String> parallelLoader;
    private ParallelLoader<String> parallelLoaderForMultiThreadedTests;
    private ParallelLoader<String> parallelLoaderWithCloseCallback;
    private final Map<Loader<String>, List<String>> loaderInteractionsMap = new HashMap<>();
    private final EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();

    // This class is used for encapsulating a boolean value that can be used by the callback tests to
    // verify if the callback was used. Needs to be in a class to allow the lambda to act as a closure.
    @Data
    @AllArgsConstructor
    class CallbackExecutedStore {
        private Boolean condition;
    }
    private CallbackExecutedStore callbackExecutedStore;

    @Before
    public void setupCallbackExecutedStore() {
        callbackExecutedStore = new CallbackExecutedStore(null);
    }

    @Before
    public void setupMockLoaderProvider() {
        when(mockLoaderSupplier.get()).thenReturn(mockLoader);
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setupMultiThreadedBufferedLoaderMocks() {
        when(mockMultiThreadedLoaderSupplier.get()).thenAnswer(factoryInvocation -> {
            Loader<String> loader = mock(Loader.class);
            loaderInteractionsMap.put(loader, new ArrayList<>());

            doAnswer(loaderInvocation -> {
                loaderInteractionsMap.get(loader).add((String) loaderInvocation.getArguments()[0]);
                Thread.sleep(10);
                return null;
            }).when(loader).load(anyString());

            return loader;
        });
    }

    @Before
    public void setUpLoader() {
        parallelLoader = ParallelLoader.of(mockLoaderSupplier);
        parallelLoaderWithCloseCallback = ParallelLoader.of(mockLoaderSupplier)
                .withOnCloseCallback((condition, metrics) -> callbackExecutedStore.setCondition(condition));
        parallelLoaderForMultiThreadedTests = ParallelLoader.of(mockMultiThreadedLoaderSupplier);
    }

    @Test
    public void callsFactoryWithCorrectParameters() throws Exception {
        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);
        parallelLoader.close();

        verify(mockLoaderSupplier, times(1)).get();
    }

    @Test
    public void loadWritesObjectToNewLoader() throws Exception {
        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);
        parallelLoader.close();

        verify(mockLoader, times(1)).load(eq(TEST_STRING));
    }

    @Test
    public void loadOpensNewLoader() throws Exception {
        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);
        parallelLoader.close();

        verify(mockLoader, times(1)).open(eq(mockMetrics));
    }

    @Test
    public void loadWillCallWriteOnThreadLocalLoaderWhenMultiThreaded() throws Exception {
        parallelLoaderForMultiThreadedTests.open(mockMetrics);
        EtlExecutor multiThreadedExecutor = etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(NUMBER_OF_LOADERS, EXECUTOR_QUEUE_SIZE);
        for (int i = 0; i < 100; i++) {
            multiThreadedExecutor.submit(() -> parallelLoaderForMultiThreadedTests.load(TEST_STRING), mockMetrics);
        }
        parallelLoaderForMultiThreadedTests.close();

        loaderInteractionsMap.keySet().forEach(loader -> {
            try {
                verify(loader, atLeastOnce()).load(TEST_STRING);
            } catch (Exception ignored) {
            }
        });
    }

    @Test
    public void closeCallsCloseOnAllLoaders() throws Exception {
        parallelLoaderForMultiThreadedTests.open(mockMetrics);
        EtlExecutor multiThreadedExecutor = etlExecutorFactory.newBlockingFixedThreadsEtlExecutor(NUMBER_OF_LOADERS, EXECUTOR_QUEUE_SIZE);
        for (int i = 0; i < 100; i++) {
            multiThreadedExecutor.submit(() -> parallelLoaderForMultiThreadedTests.load(TEST_STRING), mockMetrics);
        }
        parallelLoaderForMultiThreadedTests.close();

        loaderInteractionsMap.keySet().forEach(loader -> {
            try {
                verify(loader, times(1)).close();
            } catch (Exception ignored) {
            }
        });
    }

    @Test
    public void loadAfterCloseThrowsIllegalStateException() throws Exception {
        thrown.expect(IllegalStateException.class);
        parallelLoader.open(mockMetrics);
        parallelLoader.close();
        parallelLoader.load(TEST_STRING);
    }

    @Test
    public void closeWillNotExecuteTheCloseCallbackWhenNull() throws Exception {
        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);
        parallelLoader.close();
        assertThat(callbackExecutedStore.getCondition(), is(nullValue()));
    }

    @Test
    public void closeWillPassTrueConditionToCloseCallbackWhenDataIsLoaded() throws Exception {
        parallelLoaderWithCloseCallback.open(mockMetrics);
        parallelLoaderWithCloseCallback.load(TEST_STRING);
        parallelLoaderWithCloseCallback.close();
        assertThat(callbackExecutedStore.getCondition(), is(true));
    }

    @Test
    public void closeWillPassFalseConditionToCloseCallbackIfDataWasNotLoaded() throws Exception {
        parallelLoaderWithCloseCallback.open(mockMetrics);
        parallelLoaderWithCloseCallback.close();
        assertThat(callbackExecutedStore.getCondition(), is(false));
    }

    @Test
    public void closeWillThrowRuntimeExceptionWhenCloseCallbackThrowsException() throws Exception {
        thrown.expect(RuntimeException.class);

        parallelLoader = ParallelLoader.of(mockLoaderSupplier)
                .withOnCloseCallback((condition, metrics) -> {
            throw new IllegalArgumentException("Something done goofed!");
        });

        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);
        parallelLoader.close();
    }

    @Test
    public void loadEmitsTimingMetrics() {
        parallelLoader.open(mockMetrics);
        parallelLoader.load(TEST_STRING);

        // we just want to assert that timing metrics are being emitted by close
        verify(mockMetrics).addTime(eq("ParallelLoader.load"), anyDouble());
        verify(mockMetrics).close();
    }

    @Test
    public void closeEmitsTimingMetrics() throws Exception {
        parallelLoader.open(mockMetrics);
        parallelLoader.close();

        // we just want to assert that timing metrics are being emitted by close
        verify(mockMetrics).addTime(eq("ParallelLoader.close"), anyDouble());
        verify(mockMetrics).close();
    }
}
