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

package com.amazon.pocketEtl;

import com.amazon.pocketEtl.core.executor.EtlExecutorFactory;
import com.amazon.pocketEtl.core.producer.EtlProducer;
import com.amazon.pocketEtl.core.producer.EtlProducerFactory;
import lombok.AccessLevel;
import lombok.Getter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;

/**
 * This object provides a fluent stream interface to Pocket-ETL jobs and somewhat resembles the Java-8 streaming
 * interface but has some important behavioral differences :
 *
 * 1) Pocket ETL streams are asynchronous: each stage runs on a separate thread-pool and buffers work from the previous
 *    stages. The level of parallelism for each stage can be overridden from a default of 1, if desired.
 * 2) The streams can be combined with each other to create larger streams with parallel running extractors.
 * 3) Each stage deserializes the underlying stream data into a custom object that the stage specifies. This mapping
 *    happens automatically. In the case of a transformer the output object is serialized and merged back into the
 *    underlying data stream which allows for attribute tunnelling through stages.
 * 4) Because of the way data is serialized and deserialized between stages, primitive data-types are not supported.
 *    All data objects must be represented by a class with named fields that can serialize to a Map.
 * 5) You can pass An EtlMetrics object into the stream to capture profiling data and easily integrate the reporting
 *    of those metrics with your existing service.
 *
 *
 * This builder provides a fluent interface for expressing ETL jobs : extract(e).transform(t).load(l).
 * For each stage you provide an Extractor, Transformer and Loader object respectively. A selection of standard adapters
 * for these objects is provided. You can write your own by implementing the functional interface or simply insert a
 * lambda function.
 *
 * When multiple Extractors are provided to the extract stage, the extractors will be run in parallel and all be linked
 * to the same downstream stages, eg: E(e1,e2).T(t1).L(l1) will pipe all extracted objects from e1 and e2 to t1 and then
 * l1.
 *
 * When multiple EtlStream objects are combined using the static combine() method then all the extractors that form the
 * head of the component streams will be run in parallel when the new stream is executed. Any stages that are added onto
 * the end of a combined stream will be linked to any unterminated stages in the component streams:
 *
 * Eg: e1.t1 combined with e2.t2 will create a resulting stream that is equivalent to (e1.t1, e2.t2). If l3 is added to
 * the end of this new stream then the output of t1 and t2 will be linked to it : (e1.t1, e2.t2).l3 effectively merging
 * the output of the parallel streams into a single loader.
 *
 * Each stage of the stream can be customized with an EtlStage object that allows you to tweak the structural behavior
 * of that component such as giving it a custom identifier for logging, specify the parallelism of that stage or add
 * an additional buffer.
 *
 * Here is a real example of a simple ETL that takes a stream of simple objects, converts a string within them to lower
 * case and the collects the results in an array. This is not a particularly useful example (Java streams would do this
 * more efficiently), it is just intended to be illustrative:
 *
 * List<TestDTO> inputData = ImmutableList.of(new TestDTO("ONE"), new TestDTO("TWO"), new TestDTO("THREE"));
 * List<TestDTO> outputData = new ArrayList<>();
 *
 * EtlStream.extract(IterableExtractor.of(inputData))
 *  .then(transform(TestDTO.class, MapTransformer.of(obj -> new TestDTO(obj.getValue().toLowerCase()))).withThreads(5))
 *  .load(TestDTO.class, outputData::add)
 *  .run();
 *
 * assertThat(outputData, containsInAnyOrder(new TestDTO("one"), new TestDTO("two"), new TestDTO("three")));
 *
 * In this example the transformer works in parallel with 5 threads, however because the loader is only running on a
 * single thread it is safe to load the data into an object which is not threadsafe (in this case an ArrayList), however
 * the objects may arrive in any order due to the parallelism in the middle.
 */
@SuppressWarnings("WeakerAccess")
@Getter(AccessLevel.PRIVATE)
public class EtlStream {
    /**
     * Combine two or more EtlStream objects into a single stream. The new stream will execute the component streams
     * in parallel when run, and any stages subsequently added to this stream will be linked to all unterminated
     * component streams.
     *
     * Example (using static imports):
     * combine(extract(customerOrdersSource), extract(vendorOrdersSource)).load(Order.class, orderDatabaseLoader);
     *
     * @param streamsToCombine A collection of EtlStream objects to combine into a single stream.
     * @return A new stream that represents the composition of the supplied streams.
     */
    @Nonnull
    public static EtlStream combine(@Nonnull Collection<EtlStream> streamsToCombine) {
        return new EtlStream(EtlProducerStage.combine(streamsToCombine), areAllStreamsTerminated(streamsToCombine));
    }

    /**
     * Combine two or more EtlStream objects into a single stream. The new stream will execute the component streams
     * in parallel when run, and any stages subsequently added to this stream will be linked to all unterminated
     * component streams.
     *
     * Example (using static imports):
     * EtlStream.combine(extract(customerOrdersSource), extract(vendorOrdersSource)).load(Order.class, orderDatabaseLoader);
     *
     * @param streamsToCombine A list of EtlStream objects to combine into a single stream.
     * @return A new stream that represents the composition of the supplied streams.
     */
    @Nonnull
    public static EtlStream combine(@Nonnull EtlStream... streamsToCombine) {
        return EtlStream.combine(Arrays.asList(streamsToCombine));

    }

    /**
     * Create a new ETL stream based on an initial producer stage. A producer stage is either a combined streams
     * stage or an extractor stage. This form allows for the inclusion of additional properties on the producer that
     * cannot be specified using the short-form static constructors.
     *
     * Example (using static imports):
     * EtlStream.from(extract(customerOrdersSource).withThreads(5));
     *
     * @param producerStage An EtlProducerStage object that the new EtlStream will be created with.
     * @return A new EtlStream.
     */
    @Nonnull
    public static EtlStream from(@Nonnull EtlProducerStage producerStage) {
        return new EtlStream(producerStage, false);
    }

    /**
     * Create a new ETL stream based on an extractor. The extractor will be created with default stage configuration
     * (default stage name), use the static 'from' constructor if you need to change the configuration.
     *
     * Example:
     * EtlStream.extract(customerOrdersSource);
     *
     * @param extractor An extractor object that will be used to produce data for the stream.
     * @return A new EtlStream.
     */
    @Nonnull
    public static EtlStream extract(@Nonnull Extractor<?> extractor) {
        return new EtlStream(EtlProducerStage.extract(extractor), false);
    }

    /**
     * Create a new ETL stream based on a collection of extractors. The extractors will run in parallel and feed all
     * their data onto the single EtlStream. The extractors will be created with default stage configuration
     * (default stage name), use the static 'from' constructor if you need to change the configuration.
     *
     * Example:
     * EtlStream.extract(orderExtractorSourceList);
     *
     * @param extractors A collection of extractor objects that will be used to produce data for the stream.
     * @return A new EtlStream.
     */
    @Nonnull
    public static EtlStream extract(@Nonnull Collection<Extractor<?>> extractors) {
        return new EtlStream(EtlProducerStage.extract(extractors), false);
    }

    /**
     * Create a new ETL stream based on a list of extractors. The extractors will run in parallel and feed all
     * their data onto the single EtlStream. The extractors will be created with default stage configuration
     * (default stage name), use the static 'from' constructor if you need to change the configuration.
     *
     * Example:
     * EtlStream.extract(customerOrdersSource, vendorOrdersSource);
     *
     * @param extractors A collection of extractor objects that will be used to produce data for the stream.
     * @return A new EtlStream.
     */
    @Nonnull
    public static EtlStream extract(@Nonnull Extractor<?>... extractors) {
        return new EtlStream(EtlProducerStage.extract(extractors), false);
    }

    /**
     * Creates a new stream that is composed of the current stream with the addition of a new consumer stage added to
     * the end of it. Note that EtlStream objects are immutable, so the original stream will not be modified. This
     * generic interface allows the properties of the stage being added to be overriden.
     *
     * Example:
     * etlStream.then(load(Order.class, orderDbLoader).withThreads(5));
     *
     * @param etlStage A new consumer stage to be performed after all the existing stages in the stream.
     * @return A new stream that is a copy of the old stream with the new stage added to it.
     */
    @Nonnull
    public EtlStream then(@Nonnull EtlConsumerStage etlStage) {
        checkTermination();
        return new EtlStream(this, etlStage);
    }

    /**
     * Creates a new stream that is composed of the current stream with the addition of a new transform stage added to
     * the end of it. Note that EtlStream objects are immutable, so the original stream will not be modified. This
     * short-hand interface does not allow the properties of the transform stage being added to be overriden. Use the
     * 'then' method to add a stage with custom property overrides.
     *
     * Example:
     * etlStream.transform(Order.class, decorateOrderTransformer);
     *
     * @param objectClass The class of object to marshal the data-stream into before passing it to the transformer.
     * @param transformer A new transformer stage to be performed after all the existing stages in the stream.
     * @return A new stream that is a copy of the old stream with the new stage added to it.
     */
    @Nonnull
    public <T> EtlStream transform(@Nonnull Class<T> objectClass, @Nonnull Transformer<T, ?> transformer) {
        return then(EtlConsumerStage.transform(objectClass, transformer));
    }

    /**
     * Creates a new stream that is composed of the current stream with the addition of a new load stage added to
     * the end of it. Note that EtlStream objects are immutable, so the original stream will not be modified. This
     * short-hand interface does not allow the properties of the load stage being added to be overriden. Use the
     * 'then' method to add a stage with custom property overrides.
     *
     * Example:
     * etlStream.load(Order.class, orderDbLoader);
     *
     * @param objectClass The class of object to marshal the data-stream into before passing it to the loader.
     * @param loader A new loader stage to be performed after all the existing stages in the stream.
     * @return A new stream that is a copy of the old stream with the new stage added to it.
     */
    @Nonnull
    public <T> EtlStream load(@Nonnull Class<T> objectClass, @Nonnull Loader<T> loader) {
        return then(EtlConsumerStage.load(objectClass, loader));
    }

    /**
     * Executes the ETL stream. This method will block until the stream has completely run, which means that all the
     * extractors must be exhausted all the stages must also have their work exhausted. Only terminated streams may be
     * executed, invoking this method on an unterminated stream will throw an exception. Once a stream has been executed
     * it may not be run again, it must be reconstructed.
     * @throws Exception If something goes wrong.
     */
    public void run() throws Exception {
        run(null, DEFAULT_RUNNER_FUNCTION);
    }

    /**
     * Executes the ETL stream. This method will block until the stream has completely run, which means that all the
     * extractors must be exhausted all the stages must also have their work exhausted. Only terminated streams may be
     * executed, invoking this method on an unterminated stream will throw an exception. Once a stream has been executed
     * it may not be run again, it must be reconstructed.
     * @param parentMetrics An EtlMetrics object that will be used to store profiling information from the ETL
     *                      stream.
     * @throws Exception If something goes wrong.
     */
    public void run(@Nonnull EtlMetrics parentMetrics) throws Exception {
        run(parentMetrics, DEFAULT_RUNNER_FUNCTION);
    }

    /****************************************************************************************************************/

    private final static EtlRunner DEFAULT_RUNNER_FUNCTION = (etlProducer, parentMetrics) -> {
        // Java 8 limits try-with-resources to fresh variables (Java 9 will support effectively final variables)
        try (EtlProducer p = etlProducer) {
            p.open(parentMetrics);
            p.produce();
        }
    };

    private final EtlExecutorFactory etlExecutorFactory = new EtlExecutorFactory();
    private final EtlProducerFactory etlProducerFactory = new EtlProducerFactory(etlExecutorFactory);

    @Getter(AccessLevel.PACKAGE)
    private final EtlStageChain stageChain;
    private boolean isTerminated;

    private EtlStream(EtlProducerStage etlStreamStage, boolean isTerminated) {
        stageChain = new EtlStageChain(etlStreamStage);
        this.isTerminated = isTerminated;
    }

    private EtlStream(EtlStream fromEtlStream, EtlConsumerStage newConsumerStage) {
        stageChain = new EtlStageChain(fromEtlStream.getStageChain(), newConsumerStage);
        this.isTerminated = newConsumerStage.isTerminal();
    }

    void run(@Nullable EtlMetrics parentMetrics, @Nonnull EtlRunner runnerFunction) throws Exception {
        EtlProducer etlJob = getStageChain().constructProducer();
        runnerFunction.run(etlJob, parentMetrics);
    }

    private void checkTermination() {
        if (isTerminated) {
            throw new IllegalStateException("Stream has been terminated, no more stages can be added");
        }
    }

    private static boolean areAllStreamsTerminated(Collection<EtlStream> etlStreams) {
        return etlStreams.stream().allMatch(EtlStream::isTerminated);
    }
}
