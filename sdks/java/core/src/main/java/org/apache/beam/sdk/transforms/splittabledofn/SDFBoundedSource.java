package org.apache.beam.sdk.transforms.splittabledofn;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SDFBoundedSource<InputT, OutputT, RestrictionT> extends BoundedSource<OutputT>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SDFBoundedSource.class);
  private final DoFn<InputT, OutputT> sdf;
  private final Coder<InputT> elementCoder;
  private final Coder<OutputT> outputCoder;
  private final Coder<RestrictionT> restrictionCoder;
  private transient InputT element;
  private transient RestrictionT restriction;

  public SDFBoundedSource(
      DoFn<InputT, OutputT> sdf,
      Coder<InputT> elementCoder,
      Coder<OutputT> outputCoder,
      Coder<RestrictionT> restrictionCoder,
      InputT element) {
    this(
        sdf,
        elementCoder,
        outputCoder,
        restrictionCoder,
        element,
        DoFnInvokers.invokerFor(sdf).<RestrictionT>invokeGetInitialRestriction(element));
  }

  private SDFBoundedSource(
      DoFn<InputT, OutputT> sdf,
      Coder<InputT> elementCoder,
      Coder<OutputT> outputCoder,
      Coder<RestrictionT> restrictionCoder,
      InputT element,
      RestrictionT restriction) {
    this.sdf = sdf;
    this.elementCoder = elementCoder;
    this.outputCoder = outputCoder;
    this.restrictionCoder = restrictionCoder;
    this.element = checkNotNull(element);
    this.restriction = checkNotNull(restriction);
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    elementCoder.encode(element, out);
    restrictionCoder.encode(restriction, out);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.element = elementCoder.decode(in);
    this.restriction = restrictionCoder.decode(in);
  }

  @Override
  public void validate() {}

  @Override
  public Coder<OutputT> getDefaultOutputCoder() {
    return outputCoder;
  }

  @Override
  public List<SDFBoundedSource<InputT, OutputT, RestrictionT>> split(
      long desiredBundleSizeBytesIgnored, PipelineOptions options) throws Exception {
    final List<SDFBoundedSource<InputT, OutputT, RestrictionT>> res = new ArrayList<>();
    DoFnInvokers.invokerFor(sdf)
        .invokeSplitRestriction(
            element,
            restriction,
            new DoFn.OutputReceiver<RestrictionT>() {
              @Override
              public void output(RestrictionT output) {
                res.add(
                    new SDFBoundedSource<>(
                        sdf, elementCoder, outputCoder, restrictionCoder, element, output));
              }
            });
    return res;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0L;
  }

  @Override
  public BoundedReader<OutputT> createReader(PipelineOptions options) throws IOException {
    return new SDFBoundedReader<>(
        options, sdf, elementCoder, outputCoder, restrictionCoder, element, restriction);
  }

  @Override
  public String toString() {
    return "SDFBoundedSource{element=" + element + ", restriction=" + restriction + '}';
  }

  private static class SDFBoundedReader<InputT, OutputT, RestrictionT>
      extends BoundedReader<OutputT> {
    private final PipelineOptions options;
    private final DoFn<InputT, OutputT> sdf;
    private final InputT element;
    private final Coder<InputT> elementCoder;
    private final Coder<OutputT> outputCoder;
    private final Coder<RestrictionT> restrictionCoder;
    private RestrictionT restriction;

    private final DoFnInvoker<InputT, OutputT> invoker;
    private RestrictionTracker<RestrictionT> tracker;
    private Thread processThread;
    private final BlockingQueue<Optional<TimestampedValue<OutputT>>> values =
        new SynchronousQueue<>();
    private Optional<TimestampedValue<OutputT>> current;
    private boolean done;

    private SDFBoundedReader(
        PipelineOptions options,
        DoFn<InputT, OutputT> sdf,
        Coder<InputT> elementCoder,
        Coder<OutputT> outputCoder,
        Coder<RestrictionT> restrictionCoder,
        InputT element,
        RestrictionT restriction) {
      this.options = options;
      this.sdf = sdf;
      this.elementCoder = elementCoder;
      this.outputCoder = outputCoder;
      this.restrictionCoder = restrictionCoder;
      this.element = checkNotNull(element);
      this.restriction = checkNotNull(restriction);
      this.invoker = DoFnInvokers.invokerFor(sdf);
      this.invoker.invokeSetup();
      this.tracker = invoker.invokeNewTracker(restriction);
      this.processThread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  runProcess();
                }
              });
      processThread.start();
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    private void runProcess() {
      final ProcessContext context = new ProcessContext();
      DoFnInvoker.ArgumentProvider<InputT, OutputT> argProvider =
          new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
            @Override
            public BoundedWindow window() {
              return GlobalWindow.INSTANCE;
            }

            @Override
            public RestrictionTracker<?> restrictionTracker() {
              return tracker;
            }

            @Override
            public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
              return context;
            }

            @Override
            public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
                DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException();
            }

            @Override
            public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
                DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException();
            }

            @Override
            public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
              throw new UnsupportedOperationException();
            }

            @Override
            public State state(String stateId) {
              throw new UnsupportedOperationException();
            }

            @Override
            public Timer timer(String timerId) {
              throw new UnsupportedOperationException();
            }
          };

      invoker.invokeProcessElement(argProvider);
      outputImpl(Optional.<TimestampedValue<OutputT>>absent());
    }

    void outputImpl(Optional<TimestampedValue<OutputT>> value) {
      try {
        values.offer(value, Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private class ProcessContext extends DoFn<InputT, OutputT>.ProcessContext {
      public ProcessContext() {
        sdf.super();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return options;
      }

      @Override
      public void output(OutputT output) {
        outputWithTimestamp(output, BoundedWindow.TIMESTAMP_MIN_VALUE);
      }

      @Override
      public void outputWithTimestamp(OutputT output, Instant timestamp) {
        outputImpl(Optional.of(TimestampedValue.of(output, timestamp)));
      }

      @Override
      public void updateWatermark(Instant watermark) {
        // nothing
      }

      @Override
      public <T> void output(TupleTag<T> tag, T output) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public InputT element() {
        return element;
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Instant timestamp() {
        throw new UnsupportedOperationException();
      }

      @Override
      public PaneInfo pane() {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public boolean advance() throws IOException {
      if (done) {
        return false;
      }
      try {
        current = values.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      if (!current.isPresent()) {
        done = true;
        return false;
      }
      return true;
    }

    @Override
    public OutputT getCurrent() throws NoSuchElementException {
      return current.get().getValue();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return current.get().getTimestamp();
    }

    @Override
    public SDFBoundedSource<InputT, OutputT, RestrictionT> getCurrentSource() {
      return new SDFBoundedSource<>(
          sdf, elementCoder, outputCoder, restrictionCoder, element, restriction);
    }

    @Nullable
    @Override
    public Double getFractionConsumed() {
      return tracker.getFractionClaimed();
    }

    @Nullable
    @Override
    public BoundedSource<OutputT> splitAtFraction(double fraction) {
      // [      |    .          ]
      //             ^fraction
      // (fraction-claimed)/(1-claimed)
      double claimed = tracker.getFractionClaimed();
      LOG.info("Requesting splitAtFraction {} while claimed is {}", fraction, claimed);
      if (fraction <= claimed) {
        LOG.info("Rejecting cause before claimed");
        return null;
      }
      double fractionOfRemainder = (fraction - claimed) / (1 - claimed);
      LOG.info("Fraction of remainder is {}", fractionOfRemainder);
      RestrictionT residual =
          tracker.splitRemainderAfterFraction(fractionOfRemainder);
      if (residual == null) {
        return null;
      }
      this.restriction = tracker.currentRestriction();
      return new SDFBoundedSource<>(
          sdf, elementCoder, outputCoder, restrictionCoder, element, residual);
    }

    @Override
    public void close() throws IOException {
      tracker.checkpoint();
      while (advance()) {}
      try {
        processThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      invoker.invokeTeardown();
    }
  }
}
