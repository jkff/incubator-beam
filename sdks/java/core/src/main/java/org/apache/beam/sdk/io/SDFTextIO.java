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
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SDFBoundedSource;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SDFTextIO {
  private static final Logger LOG = LoggerFactory.getLogger(SDFTextIO.class);

  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded {@link
   * PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_SDFTextIO_Read.Builder().build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Read build();
    }

    /**
     * Reads text files that reads from the file(s) with the given filename or filename pattern.
     *
     * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
     * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
     * remote execution service).
     *
     * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return toBuilder().setFilepattern(filepattern).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
      }

      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(createSource()));
    }

    BoundedSource<String> createSource() {
      return new SDFBoundedSource<>(
          new ReadTextSDF(),
          StringUtf8Coder.of(),
          StringUtf8Coder.of(),
          SerializableCoder.of(FileAndRange.class),
          getFilepattern().get());
    }

    private static class FileAndRange
        implements HasDefaultTracker<FileAndRange, FileAndRangeTracker>, Serializable {
      final String filenameOrPattern;
      @Nullable final OffsetRange range;

      private FileAndRange(String filenameOrPattern, @Nullable OffsetRange range) {
        this.filenameOrPattern = checkNotNull(filenameOrPattern);
        this.range = range;
      }

      @Override
      public FileAndRangeTracker newTracker() {
        return new FileAndRangeTracker(this);
      }

      @Override
      public String toString() {
        return (filenameOrPattern == null) ? range.toString() : filenameOrPattern;
      }
    }

    private static class FileAndRangeTracker implements RestrictionTracker<FileAndRange> {
      private FileAndRange fileAndRange;
      private final OffsetRangeTracker offsetRangeTracker;

      public FileAndRangeTracker(FileAndRange fileAndRange) {
        this.fileAndRange = fileAndRange;
        this.offsetRangeTracker = new OffsetRangeTracker(fileAndRange.range);
      }

      @Override
      public synchronized FileAndRange currentRestriction() {
        return fileAndRange;
      }

      @Override
      public synchronized FileAndRange checkpoint() {
        OffsetRange residual = offsetRangeTracker.checkpoint();
        this.fileAndRange =
            new FileAndRange(
                fileAndRange.filenameOrPattern, offsetRangeTracker.currentRestriction());
        return new FileAndRange(fileAndRange.filenameOrPattern, residual);
      }

      public boolean tryClaim(long offset) {
        return offsetRangeTracker.tryClaim(offset);
      }

      @Override
      public double getFractionClaimed() {
        return offsetRangeTracker.getFractionClaimed();
      }

      @Override
      public synchronized FileAndRange splitRemainderAfterFraction(double fractionOfRemainder) {
        LOG.info("Requesting splitRemainderAfterFraction " + fractionOfRemainder);
        OffsetRange residual = offsetRangeTracker.splitRemainderAfterFraction(fractionOfRemainder);
        LOG.info("ORT returned null");
        if (residual == null) {
          return null;
        }
        this.fileAndRange =
            new FileAndRange(
                fileAndRange.filenameOrPattern, offsetRangeTracker.currentRestriction());
        return new FileAndRange(fileAndRange.filenameOrPattern, residual);
      }

      @Override
      public void checkDone() throws IllegalStateException {
        offsetRangeTracker.checkDone();
      }
    }

    private static class ReadTextSDF extends DoFn<String, String> {
      @ProcessElement
      public void process(ProcessContext c, FileAndRangeTracker tracker) throws Exception {
        MatchResult.Metadata metadata =
            FileSystems.matchSingleFileSpec(tracker.currentRestriction().filenameOrPattern);
        try (TextScanner scanner = new TextScanner(metadata.resourceId(), tracker, c)) {
          scanner.scan();
        }
      }

      @GetInitialRestriction
      public FileAndRange getInitialRestriction(String filepattern) {
        return new FileAndRange(filepattern, null);
      }

      @SplitRestriction
      public void splitRestriction(
          String filepattern,
          FileAndRange initialIgnored,
          OutputReceiver<FileAndRange> splitReceiver)
          throws IOException {
        MatchResult match = FileSystems.match(filepattern);
        checkArgument(
            match.status() == MatchResult.Status.OK,
            "Non-ok status for matching %s: %s",
            filepattern,
            match.status());
        for (MatchResult.Metadata metadata : match.metadata()) {
          splitReceiver.output(
              new FileAndRange(
                  metadata.resourceId().toString(), new OffsetRange(0, metadata.sizeBytes())));
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String filepatternDisplay =
          getFilepattern().isAccessible() ? getFilepattern().get() : getFilepattern().toString();
      builder.addIfNotNull(
          DisplayData.item("filePattern", filepatternDisplay).withLabel("File Pattern"));
    }

    @Override
    protected Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class TextScanner implements AutoCloseable {
    private final Read.FileAndRangeTracker tracker;
    private final Read.ReadTextSDF.ProcessContext context;
    private SeekableByteChannel channel;

    private static final int READ_BUFFER_SIZE = 8192;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private ByteString buffer = ByteString.EMPTY;
    private int startOfSeparatorInBuffer;
    private int endOfSeparatorInBuffer;
    private volatile boolean eof;

    TextScanner(
        ResourceId resourceId, Read.FileAndRangeTracker tracker, Read.ReadTextSDF.ProcessContext c)
        throws IOException {
      this.channel = ((SeekableByteChannel) FileSystems.open(resourceId));
      this.tracker = tracker;
      this.context = c;
    }

    public void scan() throws IOException {
      // If the first offset is greater than zero, we need to skip bytes until we see our
      // first separator.
      long start = tracker.currentRestriction().range.getFrom();

      long startOfNextRecord = 0;
      if (start > 0) {
        long requiredPosition = start - 1;
        channel.position(requiredPosition);
        findSeparatorBounds();
        buffer = buffer.substring(endOfSeparatorInBuffer);
        startOfNextRecord = requiredPosition + endOfSeparatorInBuffer;
        endOfSeparatorInBuffer = 0;
        startOfSeparatorInBuffer = 0;
      }

      while (true) {
        long startOfRecord = startOfNextRecord;
        findSeparatorBounds();

        // If we have reached EOF file and consumed all of the buffer then we know
        // that there are no more records.
        if (eof && buffer.size() == 0) {
          break;
        }

        if (!tracker.tryClaim(startOfRecord)) {
          break;
        }

        context.output(buffer.substring(0, startOfSeparatorInBuffer).toStringUtf8());
        buffer = buffer.substring(endOfSeparatorInBuffer);

        startOfNextRecord = startOfRecord + endOfSeparatorInBuffer;
      }

      tracker.offsetRangeTracker.markDone();
    }

    @Override
    public void close() throws Exception {
      channel.close();
    }

    /**
     * Locates the start position and end position of the next delimiter. Will consume the channel
     * till either EOF or the delimiter bounds are found.
     *
     * <p>This fills the buffer and updates the positions as follows:
     *
     * <pre>{@code
     * ------------------------------------------------------
     * | element bytes | delimiter bytes | unconsumed bytes |
     * ------------------------------------------------------
     * 0            start of          end of              buffer
     *              separator         separator           size
     *              in buffer         in buffer
     * }</pre>
     */
    private void findSeparatorBounds() throws IOException {
      int bytePositionInBuffer = 0;
      while (true) {
        if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
          startOfSeparatorInBuffer = endOfSeparatorInBuffer = bytePositionInBuffer;
          break;
        }

        byte currentByte = buffer.byteAt(bytePositionInBuffer);

        if (currentByte == '\n') {
          startOfSeparatorInBuffer = bytePositionInBuffer;
          endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;
          break;
        } else if (currentByte == '\r') {
          startOfSeparatorInBuffer = bytePositionInBuffer;
          endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;

          if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
            currentByte = buffer.byteAt(bytePositionInBuffer + 1);
            if (currentByte == '\n') {
              endOfSeparatorInBuffer += 1;
            }
          }
          break;
        }

        // Move to the next byte in buffer.
        bytePositionInBuffer += 1;
      }
    }

    /** Returns false if we were unable to ensure the minimum capacity by consuming the channel. */
    private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
      // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
      // attempt to read more bytes.
      while (buffer.size() <= minCapacity && !eof) {
        eof = channel.read(readBuffer) == -1;
        readBuffer.flip();
        buffer = buffer.concat(ByteString.copyFrom(readBuffer));
        readBuffer.clear();
      }
      // Return true if we were able to honor the minimum buffer capacity request
      return buffer.size() >= minCapacity;
    }
  }

  /** Disable construction of utility class. */
  private SDFTextIO() {}
}
