package org.apache.beam.sdk.transforms.splittabledofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SDFBoundedSourceTest {
  private static final class RangeFn extends DoFn<OffsetRange, Long> {
    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        c.output(2 * i);
        c.output(2 * i + 1);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(OffsetRange element) {
      return element;
    }
  }

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  public void testBasic() throws IOException {
    SDFBoundedSource<OffsetRange, Long, OffsetRange> source =
        new SDFBoundedSource<>(
            new RangeFn(),
            SerializableCoder.of(OffsetRange.class),
            VarLongCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            new OffsetRange(0, 2));

    PipelineOptions options = PipelineOptionsFactory.create();
    try (BoundedSource.BoundedReader<Long> reader = source.createReader(options)) {
      assertTrue(reader.start());
      assertEquals(0L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(1L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(2L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(3L, reader.getCurrent().longValue());
      assertFalse(reader.advance());
    }
  }

  @Test
  public void testCloseWithoutReading() throws IOException {
    SDFBoundedSource<OffsetRange, Long, OffsetRange> source =
        new SDFBoundedSource<>(
            new RangeFn(),
            SerializableCoder.of(OffsetRange.class),
            VarLongCoder.of(),
            SerializableCoder.of(OffsetRange.class),
            new OffsetRange(0, 2));

    PipelineOptions options = PipelineOptionsFactory.create();
    try (BoundedSource.BoundedReader<Long> reader = source.createReader(options)) {
      // Nothing
    }
  }

  @Test
  public void testReadFromSource() throws Exception {
    PCollection<Long> res =
        p.apply(
            Read.from(
                new SDFBoundedSource<>(
                    new RangeFn(),
                    SerializableCoder.of(OffsetRange.class),
                    VarLongCoder.of(),
                    SerializableCoder.of(OffsetRange.class),
                    new OffsetRange(0, 2))));
    PAssert.that(res).containsInAnyOrder(0L, 1L, 2L, 3L);
    p.run();
  }
}
