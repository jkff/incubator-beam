package org.apache.beam.sdk.transforms.splittabledofn;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
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

public class SDFUnboundedSource<InputT, OutputT, RestrictionT>
    extends UnboundedSource<OutputT, SDFUnboundedSource.RestrictionMark<RestrictionT>> {
  public static class RestrictionMark<RestrictionT> implements UnboundedSource.CheckpointMark {
    private final RestrictionT residualRestriction;

    public RestrictionMark(RestrictionT residualRestriction) {
      this.residualRestriction = residualRestriction;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // Nothing
    }
  }

  public static final class RestrictionMarkCoder<RestrictionT>
      extends AtomicCoder<RestrictionMark<RestrictionT>> {
    private final Coder<RestrictionT> restrictionCoder;

    public static <RestrictionT> RestrictionMarkCoder<RestrictionT> of(
        Coder<RestrictionT> restrictionCoder) {
      return new RestrictionMarkCoder<>(restrictionCoder);
    }

    private RestrictionMarkCoder(Coder<RestrictionT> restrictionCoder) {
      this.restrictionCoder = restrictionCoder;
    }

    @Override
    public void encode(RestrictionMark<RestrictionT> value, OutputStream outStream)
        throws IOException {
      restrictionCoder.encode(value.residualRestriction, outStream);
    }

    @Override
    public RestrictionMark<RestrictionT> decode(InputStream inStream) throws IOException {
      return new RestrictionMark<>(restrictionCoder.decode(inStream));
    }
  }

  private final DoFn<InputT, OutputT> sdf;
  private final InputT element;
  private final RestrictionT restriction;
  private final Coder<InputT> inputCoder;

  public SDFUnboundedSource(DoFn<InputT, OutputT> sdf, InputT element, Coder<InputT> inputCoder) {
    this.sdf = sdf;
    this.element = element;
    this.restriction = DoFnInvokers.invokerFor(sdf).invokeGetInitialRestriction(element);
    this.inputCoder = inputCoder;
  }

  private SDFUnboundedSource(
      DoFn<InputT, OutputT> sdf,
      InputT element,
      RestrictionT restriction,
      Coder<InputT> inputCoder) {
    this.sdf = sdf;
    this.element = element;
    this.restriction = restriction;
    this.inputCoder = inputCoder;
  }

  @Override
  public void validate() {}

  @Override
  public Coder<OutputT> getDefaultOutputCoder() {
    return null;
  }

  @Override
  public List<SDFUnboundedSource<InputT, OutputT, RestrictionT>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    final List<SDFUnboundedSource<InputT, OutputT, RestrictionT>> res = new ArrayList<>();
    DoFnInvokers.invokerFor(sdf)
        .invokeSplitRestriction(
            element,
            restriction,
            new DoFn.OutputReceiver<RestrictionT>() {
              @Override
              public void output(RestrictionT output) {
                res.add(
                    new SDFUnboundedSource<InputT, OutputT, RestrictionT>(
                        sdf, element, restriction, inputCoder));
              }
            });
    return res;
  }

  @Override
  public UnboundedReader<OutputT> createReader(
      PipelineOptions options, @Nullable RestrictionMark<RestrictionT> checkpointMark)
      throws IOException {
    return new SDFUnboundedReader<>(
        options,
        sdf,
        element,
        inputCoder, (checkpointMark == null) ? restriction : checkpointMark.residualRestriction);
  }

  @Override
  public Coder<RestrictionMark<RestrictionT>> getCheckpointMarkCoder() {
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.invokerFor(sdf).invokeGetRestrictionCoder(null);
    return RestrictionMarkCoder.of(restrictionCoder);
  }

  private static class SDFUnboundedReader<InputT, OutputT, RestrictionT>
      extends UnboundedReader<OutputT> {
    private final PipelineOptions options;
    private final DoFn<InputT, OutputT> sdf;
    private final InputT element;
    private final Coder<InputT> inputCoder;
    private RestrictionT restriction;

    private final DoFnInvoker<InputT, OutputT> invoker;
    private RestrictionTracker<RestrictionT> tracker;
    private Thread processThread;
    private final BlockingQueue<Optional<TimestampedValue<OutputT>>> values =
        new SynchronousQueue<>();
    private Optional<TimestampedValue<OutputT>> current;
    private final AtomicReference<Instant> watermark = new AtomicReference<>();

    private SDFUnboundedReader(
        PipelineOptions options,
        DoFn<InputT, OutputT> sdf,
        InputT element,
        Coder<InputT> inputCoder, RestrictionT restriction) {
      this.options = options;
      this.sdf = sdf;
      this.element = element;
      this.inputCoder = inputCoder;
      this.restriction = restriction;
      this.invoker = DoFnInvokers.invokerFor(sdf);
    }

    @Override
    public boolean start() throws IOException {
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
      values.add(Optional.<TimestampedValue<OutputT>>absent());
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
        try {
          values.offer(
              Optional.of(TimestampedValue.of(output, timestamp)), Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void updateWatermark(Instant watermark) {
        SDFUnboundedReader.this.watermark.set(watermark);
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
      try {
        current = values.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      return current.isPresent();
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
    public Instant getWatermark() {
      return watermark.get();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      RestrictionT residual = tracker.checkpoint();
      this.restriction = tracker.currentRestriction();
      return new RestrictionMark<>(residual);
    }

    @Override
    public SDFUnboundedSource<InputT, OutputT, RestrictionT> getCurrentSource() {
      return new SDFUnboundedSource<>(sdf, element, restriction, inputCoder);
    }

    @Override
    public void close() throws IOException {
      invoker.invokeTeardown();
      try {
        processThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }
}
