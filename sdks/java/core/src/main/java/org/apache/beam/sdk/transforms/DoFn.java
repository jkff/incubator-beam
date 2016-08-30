/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.OldDoFn.DelegatingAggregator;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The argument to {@link ParDo} providing the code to use to process
 * elements of the input
 * {@link org.apache.beam.sdk.values.PCollection}.
 *
 * <p>See {@link ParDo} for more explanation, examples of use, and
 * discussion of constraints on {@code DoFn}s, including their
 * serializability, lack of access to global shared mutable state,
 * requirements for failure tolerance, and benefits of optimization.
 *
 * <p>{@code DoFn}s can be tested in a particular
 * {@code Pipeline} by running that {@code Pipeline} on sample input
 * and then checking its output.  Unit testing of a {@code DoFn},
 * separately from any {@code ParDo} transform or {@code Pipeline},
 * can be done via the {@link DoFnTester} harness.
 *
 * <p>Implementations must define a method annotated with {@link ProcessElement}
 * that satisfies the requirements described there. See the {@link ProcessElement}
 * for details.
 *
 * <p>Example usage:
 *
 * <pre> {@code
 * PCollection<String> lines = ... ;
 * PCollection<String> words =
 *     lines.apply(ParDo.of(new DoFn<String, String>() {
 *         @ProcessElement
 *         public void processElement(ProcessContext c, BoundedWindow window) {
 *
 *         }}));
 * } </pre>
 *
 * @param <InputT> the type of the (main) input elements
 * @param <OutputT> the type of the (main) output elements
 */
public abstract class DoFn<InputT, OutputT> implements Serializable, HasDisplayData {

  /** Information accessible to all methods in this {@code DoFn}. */
  public abstract class Context {

    /**
     * Returns the {@code PipelineOptions} specified with the
     * {@link org.apache.beam.sdk.runners.PipelineRunner}
     * invoking this {@code DoFn}.  The {@code PipelineOptions} will
     * be the default running via {@link DoFnTester}.
     */
    public abstract PipelineOptions getPipelineOptions();

    /**
     * Adds the given element to the main output {@code PCollection}.
     *
     * <p>Once passed to {@code output} the element should not be modified in
     * any way.
     *
     * <p>If invoked from {@link ProcessElement}, the output
     * element will have the same timestamp and be in the same windows
     * as the input element passed to the method annotated with
     * {@code @ProcessElement}.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle},
     * this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element. The output element
     * will have a timestamp of negative infinity.
     */
    public abstract void output(OutputT output);

    /**
     * Adds the given element to the main output {@code PCollection},
     * with the given timestamp.
     *
     * <p>Once passed to {@code outputWithTimestamp} the element should not be
     * modified in any way.
     *
     * <p>If invoked from {@link ProcessElement}), the timestamp
     * must not be older than the input element's timestamp minus
     * {@link DoFn#getAllowedTimestampSkew}.  The output element will
     * be in the same windows as the input element.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle},
     * this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element except for the
     * timestamp.
     */
    public abstract void outputWithTimestamp(OutputT output, Instant timestamp);

    /**
     * Adds the given element to the side output {@code PCollection} with the
     * given tag.
     *
     * <p>Once passed to {@code sideOutput} the element should not be modified
     * in any way.
     *
     * <p>The caller of {@code ParDo} uses {@link ParDo#withOutputTags} to
     * specify the tags of side outputs that it consumes. Non-consumed side
     * outputs, e.g., outputs for monitoring purposes only, don't necessarily
     * need to be specified.
     *
     * <p>The output element will have the same timestamp and be in the same
     * windows as the input element passed to {@link ProcessElement}).
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle},
     * this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element. The output element
     * will have a timestamp of negative infinity.
     *
     * @see ParDo#withOutputTags
     */
    public abstract <T> void sideOutput(TupleTag<T> tag, T output);

    /**
     * Adds the given element to the specified side output {@code PCollection},
     * with the given timestamp.
     *
     * <p>Once passed to {@code sideOutputWithTimestamp} the element should not be
     * modified in any way.
     *
     * <p>If invoked from {@link ProcessElement}), the timestamp
     * must not be older than the input element's timestamp minus
     * {@link DoFn#getAllowedTimestampSkew}.  The output element will
     * be in the same windows as the input element.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle},
     * this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element except for the
     * timestamp.
     *
     * @see ParDo#withOutputTags
     */
    public abstract <T> void sideOutputWithTimestamp(
        TupleTag<T> tag, T output, Instant timestamp);
  }

  /**
   * Information accessible when running a {@link DoFn.ProcessElement} method.
   */
  public abstract class ProcessContext extends Context {

    /**
     * Returns the input element to be processed.
     *
     * <p>The element will not be changed -- it is safe to cache, etc.
     * without copying.
     */
    public abstract InputT element();


    /**
     * Returns the value of the side input.
     *
     * @throws IllegalArgumentException if this is not a side input
     * @see ParDo#withSideInputs
     */
    public abstract <T> T sideInput(PCollectionView<T> view);

    /**
     * Returns the timestamp of the input element.
     *
     * <p>See {@link org.apache.beam.sdk.transforms.windowing.Window}
     * for more information.
     */
    public abstract Instant timestamp();

    /**
     * Returns information about the pane within this window into which the
     * input element has been assigned.
     *
     * <p>Generally all data is in a single, uninteresting pane unless custom
     * triggering and/or late data has been explicitly requested.
     * See {@link org.apache.beam.sdk.transforms.windowing.Window}
     * for more information.
     */
    public abstract PaneInfo pane();
  }

  /**
   * Returns the allowed timestamp skew duration, which is the maximum
   * duration that timestamps can be shifted backward in
   * {@link DoFn.Context#outputWithTimestamp}.
   *
   * <p>The default value is {@code Duration.ZERO}, in which case
   * timestamps can only be shifted forward to future.  For infinite
   * skew, return {@code Duration.millis(Long.MAX_VALUE)}.
   */
  public Duration getAllowedTimestampSkew() {
    return Duration.ZERO;
  }

  /////////////////////////////////////////////////////////////////////////////

  protected Map<String, DelegatingAggregator<?, ?>> aggregators = new HashMap<>();

  /**
   * Protects aggregators from being created after initialization.
   */
  private boolean aggregatorsAreFinal;

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the input type of this {@code DoFn} instance's most-derived
   * class.
   *
   * <p>See {@link #getOutputTypeDescriptor} for more discussion.
   */
  protected TypeDescriptor<InputT> getInputTypeDescriptor() {
    return new TypeDescriptor<InputT>(getClass()) {};
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the output type of this {@code DoFn} instance's
   * most-derived class.
   *
   * <p>In the normal case of a concrete {@code DoFn} subclass with
   * no generic type parameters of its own (including anonymous inner
   * classes), this will be a complete non-generic type, which is good
   * for choosing a default output {@code Coder<O>} for the output
   * {@code PCollection<O>}.
   */
  protected TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return new TypeDescriptor<OutputT>(getClass()) {};
  }

  /**
   * Interface for runner implementors to provide implementations of extra context information.
   *
   * <p>The methods on this interface are called by {@link DoFnInvoker} before invoking an
   * annotated {@link StartBundle}, {@link ProcessElement} or {@link FinishBundle} method that
   * has indicated it needs the given extra context.
   *
   * <p>In the case of {@link ProcessElement} it is called once per invocation of
   * {@link ProcessElement}.
   */
  public interface ExtraContextFactory<InputT, OutputT> {
    /**
     * Construct the {@link BoundedWindow} to use within a {@link DoFn} that
     * needs it. This is called if the {@link ProcessElement} method has a parameter of type
     * {@link BoundedWindow}.
     *
     * @return {@link BoundedWindow} of the element currently being processed.
     */
    BoundedWindow window();

    /**
     * A placeholder for testing purposes.
     */
    InputProvider<InputT> inputProvider();

    /**
     * A placeholder for testing purposes.
     */
    OutputReceiver<OutputT> outputReceiver();

    /**
     * If this is a splittable {@link DoFn}, returns the {@link RestrictionTracker} associated with
     * the current {@link ProcessElement} call.
     */
    <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker();
  }

  /** A placeholder for testing handling of output types during {@link DoFn} reflection. */
  public interface OutputReceiver<T> {
    void output(T output);
  }

  /** A placeholder for testing handling of input types during {@link DoFn} reflection. */
  public interface InputProvider<T> {
    T get();
  }

  /** For testing only, this {@link ExtraContextFactory} returns {@code null} for all parameters. */
  public static class FakeExtraContextFactory<InputT, OutputT>
      implements ExtraContextFactory<InputT, OutputT> {
    @Override
    public BoundedWindow window() {
      return null;
    }

    @Override
    public InputProvider<InputT> inputProvider() {
      return null;
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver() {
      return null;
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
      return null;
    }
  }

  /////////////////////////////////////////////////////////////////////////////


  /**
   * Annotation for the method to use to prepare an instance for processing bundles of elements. The
   * method annotated with this must satisfy the following constraints
   *
   * <ul>
   *   <li>It must have zero arguments.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Setup {
  }

  /**
   * Annotation for the method to use to prepare an instance for processing a batch of elements.
   * The method annotated with this must satisfy the following constraints:
   * <ul>
   *   <li>It must have exactly one argument.
   *   <li>Its first (and only) argument must be a {@link DoFn.Context}.
   * </ul>
   *
   * <p>A simple method declaration would look like:
   * <code>
   *   public void setup(DoFn.Context c) { .. }
   * </code>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface StartBundle {}

  /**
   * Annotation for the method to use for processing elements. A subclass of {@link DoFn} must have
   * a method with this annotation.
   *
   * <p>The signature of this method must satisfy the following constraints:
   *
   * <ul>
   * <li>Its first argument must be a {@link DoFn.ProcessContext}.
   * <li>Its remaining arguments, if any, must be either {@link BoundedWindow} or a subtype of
   *     {@link RestrictionTracker}. Each of these must appear at most once.
   * <li>The {@link BoundedWindow} argument corresponds to the window of the current element. If
   *     absent, a runner may perform additional optimizations.
   * <li>If the {@link RestrictionTracker} argument is present, this {@link DoFn} is treated as <a
   *     href="https://s.apache.org/splittable-do-fn">splittable</a> and there are additional
   *     constraints on related methods {@link GetInitialRestriction}, {@link SplitRestriction},
   *     {@link NewTracker}, {@link GetRestrictionCoder}, as well as the class-level annotations
   *     {@link Bounded} and {@link Unbounded}.
   * <li>If this {@link DoFn} is splittable, the {@link ProcessElement} method <i>may</i> return a
   *     {@link ProcessContinuation} to indicate whether there is more work to be done for the
   *     current element.
   * </ul>
   *
   * <h2>Splittable DoFn's (WARNING: work in progress, do not use)</h2>
   *
   * A {@link DoFn} is <i>splittable</i> if its {@link ProcessElement} method has a parameter whose
   * type is a subtype of {@link RestrictionTracker}. This is an advanced feature and an
   * overwhelming majority of users will never need to write a splittable {@link DoFn}. Right now
   * the implementation of this feature is in progress and it's not ready for any use.
   *
   * <p>See <a href="https://s.apache.org/splittable-do-fn">the proposal</a> for an overview of the
   * involved concepts (<i>splittable DoFn</i>, <i>restriction</i>, <i>restriction tracker</i>).
   *
   * <p>If a {@link DoFn} is splittable, the following constraints must be respected:
   *
   * <ul>
   * <li>It <i>must</i> define a {@link GetInitialRestriction} method.
   * <li>It <i>may</i> define a {@link SplitRestriction} method.
   * <li>It <i>must</i> define a {@link NewTracker} method returning the same type as the type of
   *     the {@link RestrictionTracker} argument of {@link ProcessElement}, which in turn must be a
   *     subtype of {@code RestrictionTracker<R>} where {@code R} is the restriction type returned
   *     by {@link GetInitialRestriction}.
   * <li>It <i>may</i> define a {@link GetRestrictionCoder} method.
   * <li>The type of restrictions used by all of these methods must be the same.
   * <li>The {@link DoFn} itself <i>may</i> be annotated with {@link Bounded} or {@link Unbounded},
   *     but not both at the same time. If it's not annotated with either of these, it's assumed to
   *     be {@link Bounded} if its {@link ProcessElement} method returns {@code void} and {@link
   *     Unbounded} if it returns a {@link ProcessContinuation}.
   * </ul>
   *
   * A non-splittable {@link DoFn} <i>must not</i> define any of these methods.
   *
   * <p>More documentation will be added when the feature becomes ready for general usage.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface ProcessElement {}

  /**
   * Annotation for the method to use to finish processing a batch of elements.
   * The method annotated with this must satisfy the following constraints:
   * <ul>
   *   <li>It must have at least one argument.
   *   <li>Its first (and only) argument must be a {@link DoFn.Context}.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface FinishBundle {}


  /**
   * Annotation for the method to use to clean up this instance after processing bundles of
   * elements. No other method will be called after a call to the annotated method is made.
   * The method annotated with this must satisfy the following constraint:
   * <ul>
   *   <li>It must have zero arguments.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Teardown {
  }

  /**
   * Annotation for the method that maps an element to an initial restriction for a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code RestrictionT getInitialRestriction(InputT element);}
   *
   * TODO: Make the InputT parameter optional.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetInitialRestriction {}

  /**
   * Annotation for the method that returns the coder to use for the restriction of a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>If not defined, a coder will be inferred using standard coder inference rules.
   *
   * <p>Signature: {@code Coder<RestrictionT> getRestrictionCoder();}
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetRestrictionCoder {}

  /**
   * Annotation for the method that splits restriction of a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} into multiple parts to
   * be processed in parallel.
   *
   * <p>Signature: {@code List<RestrictionT> splitRestriction(
   *   InputT element, RestrictionT restriction, int numParts);}
   *
   * <p>{@code numParts} is a hint as to the number of parts into which the restriction should be
   * split. The method is allowed to respect the hint or to ignore it and produce any number of
   * parts. 0 means "no hint, choose a reasonable value".
   *
   * <p>Optional: if this method is omitted, the restriction will not be split (equivalent to
   * defining the method and returning {@code Collections.singletonList(restriction)}).
   *
   * TODO: Make the InputT and numParts parameters optional.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface SplitRestriction {
    /**
     * Means: the {@link SplitRestriction} method should choose a reasonable number of parts
     * to split the restriction into.
     */
    int UNSPECIFIED_NUM_PARTS = 0;
  }

  /**
   * Annotation for the method that creates a new {@link RestrictionTracker} for the restriction of
   * a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code MyRestrictionTracker newTracker(RestrictionT restriction);} where {@code
   * MyRestrictionTracker} must be a subtype of {@code RestrictionTracker<RestrictionT>}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface NewTracker {}

  /**
   * Annotation on a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}
   * specifying that the {@link DoFn} performs a bounded amount of work per input element, so
   * applying it to a bounded {@link PCollection} will produce also a bounded {@link PCollection}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface Bounded {}

  /**
   * Annotation on a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}
   * specifying that the {@link DoFn} performs an unbounded amount of work per input element, so
   * applying it to a bounded {@link PCollection} will produce an unbounded {@link PCollection}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface Unbounded {}

  /**
   * When used as a return value of {@link ProcessElement}, indicates whether there is more work to
   * be done for the current element.
   */
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public static class ProcessContinuation {
    private static final ProcessContinuation STOP = new ProcessContinuation(null, null);

    /** Indicates that there is no more work to be done for the current element. */
    public static ProcessContinuation stop() {
      return STOP;
    }

    /** Indicates that there is more work to be done for the current element. */
    public static ProcessContinuation resume() {
      return new ProcessContinuation(Duration.ZERO, null);
    }

    private final Duration resumeDelay;
    @Nullable private final Instant futureOutputWatermark;

    private ProcessContinuation(Duration resumeDelay, @Nullable Instant futureOutputWatermark) {
      this.resumeDelay = resumeDelay;
      this.futureOutputWatermark = futureOutputWatermark;
    }

    /**
     * A minimum duration that should elapse between the end of this {@link ProcessElement} call and
     * the {@link ProcessElement} call continuing processing of the same element. By default, zero.
     */
    public Duration getResumeDelay() {
      return resumeDelay;
    }

    /**
     * A lower bound on timestamps of the output that will be emitted by future {@link
     * ProcessElement} calls continuing processing of the current element.
     *
     * <p>By default, equivalent to timestamp of the input element.
     */
    @Nullable
    public Instant getFutureOutputWatermark() {
      return futureOutputWatermark;
    }

    /** Builder method to set the value of {@link #getResumeDelay()}. */
    public ProcessContinuation withResumeDelay(Duration resumeDelay) {
      return new ProcessContinuation(resumeDelay, futureOutputWatermark);
    }

    /** Builder method to set the value of {@link #getFutureOutputWatermark()}. */
    public ProcessContinuation withFutureOutputWatermark(Instant futureOutputWatermark) {
      return new ProcessContinuation(resumeDelay, futureOutputWatermark);
    }
  }

  /**
   * Returns an {@link Aggregator} with aggregation logic specified by the
   * {@link CombineFn} argument. The name provided must be unique across
   * {@link Aggregator}s created within the {@link DoFn}. Aggregators can only be created
   * during pipeline construction.
   *
   * @param name the name of the aggregator
   * @param combiner the {@link CombineFn} to use in the aggregator
   * @return an aggregator for the provided name and combiner in the scope of
   *         this {@link DoFn}
   * @throws NullPointerException if the name or combiner is null
   * @throws IllegalArgumentException if the given name collides with another
   *         aggregator in this scope
   * @throws IllegalStateException if called during pipeline execution.
   */
  public final <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT>
      createAggregator(String name, Combine.CombineFn<? super AggInputT, ?, AggOutputT> combiner) {
    checkNotNull(name, "name cannot be null");
    checkNotNull(combiner, "combiner cannot be null");
    checkArgument(!aggregators.containsKey(name),
        "Cannot create aggregator with name %s."
        + " An Aggregator with that name already exists within this scope.",
        name);
    checkState(!aggregatorsAreFinal,
        "Cannot create an aggregator during pipeline execution."
        + " Aggregators should be registered during pipeline construction.");

    DelegatingAggregator<AggInputT, AggOutputT> aggregator =
        new DelegatingAggregator<>(name, combiner);
    aggregators.put(name, aggregator);
    return aggregator;
  }

  /**
   * Returns an {@link Aggregator} with the aggregation logic specified by the
   * {@link SerializableFunction} argument. The name provided must be unique
   * across {@link Aggregator}s created within the {@link DoFn}. Aggregators can only be
   * created during pipeline construction.
   *
   * @param name the name of the aggregator
   * @param combiner the {@link SerializableFunction} to use in the aggregator
   * @return an aggregator for the provided name and combiner in the scope of
   *         this {@link DoFn}
   * @throws NullPointerException if the name or combiner is null
   * @throws IllegalArgumentException if the given name collides with another
   *         aggregator in this scope
   * @throws IllegalStateException if called during pipeline execution.
   */
  public final <AggInputT> Aggregator<AggInputT, AggInputT> createAggregator(
      String name, SerializableFunction<Iterable<AggInputT>, AggInputT> combiner) {
    checkNotNull(combiner, "combiner cannot be null.");
    return createAggregator(name, Combine.IterableCombineFn.of(combiner));
  }

  /**
   * Finalize the {@link DoFn} construction to prepare for processing.
   * This method should be called by runners before any processing methods.
   */
  public void prepareForProcessing() {
    aggregatorsAreFinal = true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method
   * to provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
  }
}
