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
import com.amazon.pocketEtl.extractor.IterableExtractor;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static com.amazon.pocketEtl.EtlConsumerStage.load;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class LoggingStrategyFunctionalTest {
    private Logger logger;
    private Appender appender;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setup() {
        logger = (Logger) LogManager.getRootLogger();
        appender = OutputStreamAppender.newBuilder().setName("Appender").setTarget(out).build();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
    }

    @After
    public void tearDown() {
        appender.stop();
        logger.removeAppender(appender);
    }

    @Test
    public void testWithLoggingStrategy() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"));
        EtlStream.extract(IterableExtractor.of(inputData))
                .then(load(SimplePojo.class, obj -> throwRuntimeException()).withObjectLogger(SimplePojo::getA))
                .run();

        assertThat(out.toString(), containsString("SampleSubstring"));
    }

    @Test
    public void testAndLoggingStrategy() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"));
        EtlStream.extract(IterableExtractor.of(inputData))
                .then(load(SimplePojo.class, obj -> throwRuntimeException())
                        .withName("TestName")
                        .withObjectLogger(SimplePojo::getA))
                .run();

        assertThat(out.toString(), containsString("SampleSubstring"));
    }

    @Test
    public void testDefaultLoggingStrategyDoesNotRevealSensitiveInfoIfNoStrategyGiven() throws Exception {
        List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"));
        EtlStream.extract(IterableExtractor.of(inputData))
                .load(SimplePojo.class, (obj -> throwRuntimeException()))
                .run();

        assertThat(out.toString(), not(containsString("ONE")));
    }

    @Test
    public void testHappyCaseCustomLoggingStrategyAfterCombine() throws Exception {
        List<TestDTO> inputData1 = ImmutableList.of(new TestDTO("ONE"));
        List<TestDTO> inputData2 = ImmutableList.of(new TestDTO("TWO"));

        EtlStream stream1 = EtlStream.extract(IterableExtractor.of(inputData1));
        EtlStream stream2 = EtlStream.extract(IterableExtractor.of(inputData2));

        EtlStream.combine(stream1, stream2)
                .then(load(SimplePojo.class, obj -> throwRuntimeException()).withObjectLogger(SimplePojo::getA))
                .run();

        assertThat(out.toString(), containsString("SampleSubstring"));
    }

    @Test
    public void testHappyCaseDefaultLoggingStrategyDoesNotRevealInfoAfterCombine() throws Exception {
        List<TestDTO> inputData1 = ImmutableList.of(new TestDTO("ONE"));
        List<TestDTO> inputData2 = ImmutableList.of(new TestDTO("TWO"));

        EtlStream stream1 = EtlStream.extract(IterableExtractor.of(inputData1));
        EtlStream stream2 = EtlStream.extract(IterableExtractor.of(inputData2));

        EtlStream.combine(stream1, stream2)
                .load(SimplePojo.class, (obj -> throwRuntimeException())).run();

        assertThat(out.toString(), not(containsString("ONE")));
        assertThat(out.toString(), not(containsString("TWO")));
    }

    private void throwRuntimeException() {
        throw new RuntimeException();
    }

    static class SimplePojo {
        String getA() {
            return "SampleSubstring";
        }
    }
}
