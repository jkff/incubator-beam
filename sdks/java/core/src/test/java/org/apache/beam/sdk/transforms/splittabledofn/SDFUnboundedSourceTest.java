package org.apache.beam.sdk.transforms.splittabledofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SDFUnboundedSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SDFUnboundedSourceTest {
  private final class RangeFn extends DoFn<OffsetRange, Long> {
    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        c.output(i);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(OffsetRange element) {
      return element;
    }
  }

  @Test
  public void testBasic() throws IOException {
    SDFUnboundedSource<OffsetRange, Long, OffsetRange> source =
        new SDFUnboundedSource<>(
            new RangeFn(), new OffsetRange(0, 5), SerializableCoder.of(OffsetRange.class));

    PipelineOptions options = PipelineOptionsFactory.create();
    try (UnboundedSource.UnboundedReader<Long> reader = source.createReader(options, null)) {
      assertTrue(reader.start());
      assertEquals(0L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(1L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(2L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(3L, reader.getCurrent().longValue());
      assertTrue(reader.advance());
      assertEquals(4L, reader.getCurrent().longValue());
      assertFalse(reader.advance());
    }
  }
}
