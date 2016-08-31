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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * A utility transform that executes a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} by expanding it into a
 * network of simpler transforms: pair with restriction, split restrictions, assign unique key and
 * group by it (in order to get access to per-key state and timers), then process.
 *
 * <p>This transform is intended as a helper for internal use by runners when implementing {@code
 * ParDo.of(splittable DoFn)}, but not for direct use by pipeline writers.
 */
public class SplittableParDo<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
  private final DoFn<InputT, OutputT> fn;

  /**
   * Creates the transform for the given original {@link ParDo} and {@link DoFn}.
   *
   * @param name Name for the transform
   * @param fn The splittable {@link DoFn} inside the original {@link ParDo} transform.
   */
  public SplittableParDo(String name, DoFn<InputT, OutputT> fn) {
    super(name);
    this.fn = fn;
  }

  @Override
  public PCollection<OutputT> apply(PCollection<InputT> input) {
    PCollection.IsBounded isFnBounded =
        DoFnSignatures.INSTANCE.getOrParseSignature(fn.getClass()).isBounded();
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeGetRestrictionCoder();
    KvCoder<InputT, RestrictionT> splitCoder = KvCoder.of(input.getCoder(), restrictionCoder);

    return input
        .apply(
            "Pair with initial restriction",
            ParDo.of(new PairWithRestrictionFn<InputT, OutputT, RestrictionT>(fn)))
        .setCoder(splitCoder)
        .apply("Split restriction", ParDo.of(new SplitRestrictionFn<InputT, RestrictionT>(fn)))
        .apply(
            "Assign unique key", ParDo.of(new AssignRandomUniqueKeyFn<KV<InputT, RestrictionT>>()))
        .apply("Group by key", new GBKIntoKeyedWorkItems<String, KV<InputT, RestrictionT>>())
        .apply(
            "Process",
            ParDo.of(
                new ProcessFn<InputT, RestrictionT, OutputT, TrackerT>(
                    fn,
                    input.getCoder(),
                    input.getWindowingStrategy().getWindowFn().windowCoder())))
        .setIsBoundedInternal(input.isBounded().and(isFnBounded));
  }

  /**
   * Assigns a random unique key to each element of the input collection, so that the output
   * collection is effectively the same elements as input, but with access to per-element (per-key)
   * state and timers.
   */
  private static class AssignRandomUniqueKeyFn<T> extends DoFn<T, KV<String, T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(KV.of(UUID.randomUUID().toString(), c.element()));
    }
  }

  /**
   * Pairs each input element with its initial restriction using the given splittable {@link DoFn}.
   */
  private static class PairWithRestrictionFn<InputT, OutputT, RestrictionT>
      extends DoFn<InputT, KV<InputT, RestrictionT>> {
    private DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    PairWithRestrictionFn(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          KV.of(
              context.element(),
              invoker.<RestrictionT>invokeGetInitialRestriction(context.element())));
    }
  }

  /**
   * The heart of splittable {@link DoFn} execution: processes a single (element, restriction) pair
   * by creating a tracker for the restriction and checkpointing/resuming processing later if
   * necessary.
   *
   * <p>TODO: This uses deprecated OldDoFn since DoFn does not provide access to state/timer
   * internals. This should be rewritten to use the <a href="https://s.apache.org/beam-state">State
   * and Timers API</a> once it is available.
   */
  private static class ProcessFn<
          InputT, RestrictionT, OutputT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends OldDoFn<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>
      implements OldDoFn.RequiresWindowAccess {
    /**
     * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
     * acquired during the first {@link DoFn.ProcessElement} call for this element and restriction,
     * and is released when the {@link DoFn.ProcessElement} call returns {@link
     * DoFn.ProcessContinuation#stop}.
     *
     * <p>A hold is needed to avoid letting the output watermark immediately progress together with
     * the input watermark when the first {@link DoFn.ProcessElement} call completes.
     *
     * <p>The hold is updated with the future output watermark reported by ProcessContinuation.
     */
    private static final StateTag<Object, WatermarkHoldState<GlobalWindow>> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal(
                "hold", OutputTimeFns.outputAtLatestInputTimestamp()));

    /**
     * The state cell containing a copy of the element. Written during the first {@link
     * DoFn.ProcessElement} call and read during subsequent calls in response to timer firings, when
     * the original element is no longer available.
     */
    private final StateTag<Object, ValueState<WindowedValue<InputT>>> elementTag;

    /**
     * The state cell containing a restriction representing the unprocessed part of work for this
     * element.
     */
    private StateTag<Object, ValueState<RestrictionT>> restrictionTag;

    private final DoFn<InputT, OutputT> fn;

    private transient DoFnInvoker<InputT, OutputT> invoker;

    ProcessFn(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<? extends BoundedWindow> windowCoder) {
      this.fn = fn;
      elementTag =
          StateTags.value("element", WindowedValue.getFullCoder(elementCoder, windowCoder));
    }

    @Override
    public void setup() throws Exception {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
      restrictionTag =
          StateTags.value("restriction", invoker.<RestrictionT>invokeGetRestrictionCoder());
    }

    @Override
    public void processElement(final ProcessContext c) {
      final WindowingInternals<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> internals =
          c.windowingInternals();
      StateInternals<?> stateInternals = internals.stateInternals();
      TimerInternals timerInternals = internals.timerInternals();

      ValueState<WindowedValue<InputT>> elementState =
          stateInternals.state(StateNamespaces.global(), elementTag);
      ValueState<RestrictionT> restrictionState =
          stateInternals.state(StateNamespaces.global(), restrictionTag);
      WatermarkHoldState<GlobalWindow> holdState =
          stateInternals.state(StateNamespaces.global(), watermarkHoldTag);

      elementState.readLater();
      restrictionState.readLater();

      final WindowedValue<InputT> element;
      RestrictionT restriction = restrictionState.read();
      final boolean wasFirstCall = (restriction == null);
      if (wasFirstCall) {
        // This is the first ProcessElement call for this element/restriction.
        // The element and restriction are available in c.element().
        WindowedValue<KV<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(c.element().elementsIterable());
        element = windowedValue.withValue(windowedValue.getValue().getKey());
        restriction = windowedValue.getValue().getValue();
      } else {
        // This is not the first ProcessElement call for this element/restriction - rather,
        // this is a timer firing, so we need to fetch the element and restriction from state.
        element = elementState.read();
      }

      final TrackerT tracker = invoker.invokeNewTracker(restriction);
      @SuppressWarnings("unchecked")
      final RestrictionT[] residual = (RestrictionT[]) new Object[1];
      // TODO: Only let the call run for a limited amount of time, rather than simply
      // producing a limited amount of output.
      DoFn.ProcessContinuation cont =
          invoker.invokeProcessElement(
              makeContext(c, element, tracker, residual, internals), wrapTracker(tracker));

      if (cont == null) {
        // All work for this element/restriction is completed. Clear state and release hold.
        elementState.clear();
        restrictionState.clear();
        holdState.clear();
        return;
      }
      if (residual[0] == null) {
        // This means the call completed unsolicited, and the context produced by makeContext()
        // did not take a checkpoint. Take one now.
        residual[0] = checkNotNull(tracker.checkpoint());
      }

      // Save state for resuming.
      if (wasFirstCall) {
        elementState.write(element);
      }
      restrictionState.write(residual[0]);
      if (cont.getFutureOutputWatermark() != null) {
        holdState.add(cont.getFutureOutputWatermark());
      }
      // Set a timer to continue processing this element.
      timerInternals.setTimer(
          TimerInternals.TimerData.of(
              StateNamespaces.global(),
              Instant.now().plus(cont.getResumeDelay()),
              TimeDomain.PROCESSING_TIME));
    }

    private DoFn<InputT, OutputT>.ProcessContext makeContext(
        final ProcessContext baseContext,
        final WindowedValue<InputT> element,
        final TrackerT tracker,
        final RestrictionT[] residualRestrictionHolder,
        final WindowingInternals<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>
            internals) {
      return fn.new ProcessContext() {
        // Commit at least once every 10k output records.  This keeps the watermark advancing
        // smoothly, and ensures that not too much work will have to be reprocessed in the event of
        // a crash.
        // TODO: Also commit at least once every N seconds (runner-specific parameter).
        private static final int MAX_OUTPUTS_PER_BUNDLE = 10000;

        private int numOutputs = 0;

        public InputT element() {
          return element.getValue();
        }

        public Instant timestamp() {
          return element.getTimestamp();
        }

        public PaneInfo pane() {
          return element.getPane();
        }

        public void output(OutputT output) {
          internals.outputWindowedValue(
              output, element.getTimestamp(), element.getWindows(), element.getPane());
          noteOutput();
        }

        public void outputWithTimestamp(OutputT output, Instant timestamp) {
          internals.outputWindowedValue(output, timestamp, element.getWindows(), element.getPane());
          noteOutput();
        }

        private void noteOutput() {
          if (++numOutputs > MAX_OUTPUTS_PER_BUNDLE) {
            // Request a checkpoint. The fn *may* produce more output, but hopefully not too much.
            residualRestrictionHolder[0] = tracker.checkpoint();
          }
        }

        public <T> T sideInput(PCollectionView<T> view) {
          return baseContext.sideInput(view);
        }

        public PipelineOptions getPipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        public <T> void sideOutput(TupleTag<T> tag, T output) {
          // TODO: I'm not sure this is correct, but there's no "internals.sideOutputWindowedValue".
          baseContext.sideOutputWithTimestamp(tag, output, element.getTimestamp());
        }

        public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
          // TODO: I'm not sure this is correct, but there's no "internals.sideOutputWindowedValue".
          baseContext.sideOutputWithTimestamp(tag, output, timestamp);
        }
      };
    }

    /** Creates an {@link DoFn.ExtraContextFactory} that provides just the given tracker. */
    private DoFn.ExtraContextFactory<InputT, OutputT> wrapTracker(final TrackerT tracker) {
      return new DoFn.ExtraContextFactory<InputT, OutputT>() {
        @Override
        public BoundedWindow window() {
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public DoFn.InputProvider<InputT> inputProvider() {
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public DoFn.OutputReceiver<OutputT> outputReceiver() {
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public TrackerT restrictionTracker() {
          return tracker;
        }
      };
    }
  }

  /** Splits the restriction using the given {@link DoFn.SplitRestriction} method. */
  private static class SplitRestrictionFn<InputT, RestrictionT>
      extends DoFn<KV<InputT, RestrictionT>, KV<InputT, RestrictionT>> {
    private final DoFn<InputT, ?> fn;
    private transient DoFnInvoker<InputT, ?> invoker;

    SplitRestrictionFn(DoFn<InputT, ?> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<RestrictionT> parts =
          invoker.invokeSplitRestriction(
              c.element().getKey(), c.element().getValue(), SplitRestriction.UNSPECIFIED_NUM_PARTS);
      for (RestrictionT part : parts) {
        c.output(KV.of(c.element().getKey(), part));
      }
    }
  }
}
