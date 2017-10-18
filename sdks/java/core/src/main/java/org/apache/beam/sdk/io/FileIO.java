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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.io.DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE;
import static org.apache.beam.sdk.transforms.Contextful.fn;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General-purpose transforms for working with files: listing files (matching), reading and writing.
 *
 * <h2>Matching filepatterns</h2>
 *
 * <p>{@link #match} and {@link #matchAll} match filepatterns (respectively either a single
 * filepattern or a {@link PCollection} thereof) and return the files that match them as {@link
 * PCollection PCollections} of {@link MatchResult.Metadata}. Configuration options for them are in
 * {@link MatchConfiguration} and include features such as treatment of filepatterns that don't
 * match anything and continuous incremental matching of filepatterns (watching for new files).
 *
 * <h3>Example: Watching a single filepattern for new files</h3>
 *
 * <p>This example matches a single filepattern repeatedly every 30 seconds, continuously returns
 * new matched files as an unbounded {@code PCollection<Metadata>} and stops if no new files appear
 * for 1 hour.
 *
 * <pre>{@code
 * PCollection<Metadata> matches = p.apply(FileIO.match()
 *     .filepattern("...")
 *     .continuously(
 *       Duration.standardSeconds(30), afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Example: Matching a PCollection of filepatterns arriving from Kafka</h3>
 *
 * <p>This example reads filepatterns from Kafka and matches each one as it arrives, producing again
 * an unbounded {@code PCollection<Metadata>}, and failing in case the filepattern doesn't match
 * anything.
 *
 * <pre>{@code
 * PCollection<String> filepatterns = p.apply(KafkaIO.read()...);
 *
 * PCollection<Metadata> matches = filepatterns.apply(FileIO.matchAll()
 *     .withEmptyMatchTreatment(DISALLOW));
 * }</pre>
 *
 * <h2>Reading files</h2>
 *
 * <p>{@link #readMatches} converts each result of {@link #match} or {@link #matchAll} to a {@link
 * ReadableFile} that is convenient for reading a file's contents, optionally decompressing it.
 *
 * <h3>Example: Returning filenames and contents of compressed files matching a filepattern</h3>
 *
 * <p>This example matches a single filepattern and returns {@code KVs} of filenames and their
 * contents as {@code String}, decompressing each file with GZIP.
 *
 * <pre>{@code
 * PCollection<KV<String, String>> filesAndContents = p
 *     .apply(FileIO.match().filepattern("hdfs://path/to/*.gz"))
 *     // withCompression can be omitted - by default compression is detected from the filename.
 *     .apply(FileIO.readMatches().withCompression(GZIP))
 *     .apply(MapElements
 *         // uses imports from TypeDescriptors
 *         .into(KVs(strings(), strings()))
 *         .via((ReadableFile f) -> KV.of(
 *             f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String())));
 * }</pre>
 *
 * <h2>Writing files</h2>
 *
 * <p>{@link #write} and {@link #writeDynamic} write elements from a {@link PCollection} of a given
 * type to files, using a given {@link Sink} to write a set of elements to each file. The collection
 * can be bounded or unbounded - in either case, writing happens by default per window and pane, and
 * the amount of data in each window and pane is finite, so a finite number of files ("shards") are
 * written for each window and pane. There are several aspects to this process:
 *
 * <ul>
 * <li><b>How many shards are generated per pane:</b> This is controlled by <i>sharding</i>, using
 *     {@link Write#withNumShards} or {@link Write#withSharding}. The default is runner-specific, so
 *     the number of shards will vary based on runner behavior, though at least 1 shard will always
 *     be produced for every non-empty pane. Note that setting a fixed number of shards can hurt
 *     performance: it adds an additional {@link GroupByKey} to the pipeline.
 * <li><b>How the shards are named:</b> This is controlled by a {@link Write.FilenamePolicy}:
 *     filenames can depend on a variety of inputs, e.g. the window, the pane, total number of
 *     shards and the current file's shard index, etc. - see {@link Write.FilenameContext}. The
 *     standard policies are {@link Write#nameFilesUsingWindowPaneAndShard} and {@link
 *     Write#nameFilesUsingOnlyShardIgnoringWindow}. A concise way to create a custom policy
 *     is {@link Write#nameFilesUsingShardTemplate}.
 * <li><b>Which elements go into which shard:</b> It is not possible to control this - elements
 *     within a window get distributed into different shards created for that window arbitrarily,
 *     though {@link FileIO.Write} attempts to make shards approximately evenly sized. For more
 *     control over which elements go into which files, consider using <i>dynamic destinations</i>
 *     (see below).
 * <li><b>How a given set of elements is written to a shard:</b> This is controlled by the {@link
 *     Sink}, e.g. {@link AvroIO#sink} will generate Avro files. The {@link Sink} controls the
 *     format of a single file: how to open a file, how to write each element to it, and how to
 *     close the file - but it does not control the set of files or which elements go where.
 *     Elements are written to a shard in an arbitrary order. {@link FileIO.Write} can additionally
 *     compress the generated files using {@link FileIO.Write#withCompression}.
 * <li><b>How all of the above can be element-dependent:</b> This is controlled by <i>dynamic
 *     destinations</i>. It is possible to have different groups of elements use different policies
 *     for naming files and for configuring the {@link Sink}. See "dynamic destinations" below.
 * </ul>
 *
 * <h3>Dynamic destinations</h3>
 *
 * <p>If the elements in the input collection can be partitioned into groups that should be treated
 * differently, {@link FileIO.Write} supports different treatment per group ("destination"). It can
 * use different filename policies for different groups, and can differently configure the {@link
 * Sink}, e.g. write different elements to Avro files in different directories with different
 * schemas.
 *
 * <p>This feature is supported by {@link #writeDynamic}. Use {@link Write#by} to specify how to
 * partition the elements into groups ("destinations"). Then elements will be grouped by
 * destination, and {@link Write#to(Contextful)} and the {@link Write#via(Contextful)} will be
 * applied separately within each group. Note that currently sharding can not be
 * destination-dependent: every window/pane for every destination will use the same number of shards
 * specified via {@link Write#withNumShards} or {@link Write#withSharding}.
 *
 * <h3>Writing custom types to sinks</h3>
 *
 * <p>Normally, when writing a collection of a custom type using a {@link Sink} that takes a
 * different type (for example, writing a {@code PCollection<Event>} to a text-based {@code
 * Sink<String>}), one can simply apply a {@code ParDo} or {@code MapElements} to convert the custom
 * type to the sink's <i>output type</i>.
 *
 * <p>However, when using dynamic destinations, in many such cases the destination needs to be
 * extract from the original type, so such a conversion is not possible. For example, one might
 * write events of a custom class {@code Event} to a text sink, using the event's "type" as a
 * destination. In that case, specify an <i>output function</i> in {@link Write#via(Contextful,
 * Contextful)} or {@link Write#via(Contextful, Sink)}.
 *
 * <h3>Example: Writing CSV files</h3>
 *
 * <pre>{@code
 * class CSVSink implements FileSink<List<String>> {
 *   private String header;
 *   private PrintWriter writer;
 *
 *   public CSVSink(List<String> colNames) {
 *     this.header = Joiner.on(",").join(colNames);
 *   }
 *
 *   public void open(WritableByteChannel channel) throws IOException {
 *     writer = new PrintWriter(Channels.newOutputStream(channel));
 *     writer.println(header);
 *   }
 *
 *   public void write(List<String> element) throws IOException {
 *     writer.println(Joiner.on(",").join(element));
 *   }
 *
 *   public void finish() throws IOException {
 *     writer.flush();
 *   }
 * }
 *
 * PCollection<BankTransaction> transactions = ...;
 * // Convert transactions to strings before writing them to the CSV sink.
 * transactions.apply(MapElements
 *         .into(lists(strings()))
 *         .via(tx -> Arrays.asList(tx.getUser(), tx.getAmount())))
 *     .apply(FileIO.<List<String>>write()
 *         .via(new CSVSink(Arrays.asList("user", "amount"))
 *         .to(prefixAndWindowedShard(".../path/to/transactions")))
 * }</pre>
 *
 * <h3>Example: Writing CSV files to different directories and with different headers</h3>
 *
 * <pre>{@code
 * enum TransactionType {
 *   DEPOSIT,
 *   WITHDRAWAL,
 *   TRANSFER,
 *   ...
 *
 *   List<String> getFieldNames();
 *   List<String> getAllFields(BankTransaction tx);
 * }
 *
 * PCollection<BankTransaction> transactions = ...;
 * transactions.apply(FileIO.<TransactionType, Transaction>writeDynamic()
 *     .by(Transaction::getType)
 *     .via(tx -> tx.getType().toFields(tx),  // Convert the data to be written to CSVSink
 *          type -> new CSVSink(type.getFieldNames()))
 *     .to(type -> prefixAndWindowedShard(".../path/to/" + type + "-transactions")));
 * }</pre>
 */
public class FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(FileIO.class);

  /**
   * Matches a filepattern using {@link FileSystems#match} and produces a collection of matched
   * resources (both files and directories) as {@link MatchResult.Metadata}.
   *
   * <p>By default, matches the filepattern once and produces a bounded {@link PCollection}. To
   * continuously watch the filepattern for new matches, use {@link MatchAll#continuously(Duration,
   * TerminationCondition)} - this will produce an unbounded {@link PCollection}.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#DISALLOW}. To configure this behavior, use {@link
   * Match#withEmptyMatchTreatment}.
   */
  public static Match match() {
    return new AutoValue_FileIO_Match.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * Like {@link #match}, but matches each filepattern in a collection of filepatterns.
   *
   * <p>Resources are not deduplicated between filepatterns, i.e. if the same resource matches
   * multiple filepatterns, it will be produced multiple times.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#ALLOW_IF_WILDCARD}. To configure this behavior, use {@link
   * MatchAll#withEmptyMatchTreatment}.
   */
  public static MatchAll matchAll() {
    return new AutoValue_FileIO_MatchAll.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  /**
   * Converts each result of {@link #match} or {@link #matchAll} to a {@link ReadableFile} which can
   * be used to read the contents of each file, optionally decompressing it.
   */
  public static ReadMatches readMatches() {
    return new AutoValue_FileIO_ReadMatches.Builder()
        .setCompression(Compression.AUTO)
        .setDirectoryTreatment(ReadMatches.DirectoryTreatment.SKIP)
        .build();
  }

  /** Writes elements to files using a {@link Sink}. See class-level documentation. */
  public static <InputT> Write<Void, InputT> write() {
    return FileIO.<Void, InputT>writeDynamic()
        .by(fn(SerializableFunctions.<InputT, Void>constant(null)))
        .withDestinationCoder(VoidCoder.of());
  }

  /**
   * Writes elements to files using a {@link Sink} and grouping the elements using "dynamic
   * destinations". See class-level documentation.
   */
  public static <DestT, InputT> Write<DestT, InputT> writeDynamic() {
    return new AutoValue_FileIO_Write.Builder<DestT, InputT>()
        .setCompression(Compression.UNCOMPRESSED)
        .setIgnoreWindowing(false)
        .build();
  }

  /** A utility class for accessing a potentially compressed file. */
  public static final class ReadableFile {
    private final MatchResult.Metadata metadata;
    private final Compression compression;

    ReadableFile(MatchResult.Metadata metadata, Compression compression) {
      this.metadata = metadata;
      this.compression = compression;
    }

    /** Returns the {@link MatchResult.Metadata} of the file. */
    public MatchResult.Metadata getMetadata() {
      return metadata;
    }

    /** Returns the method with which this file will be decompressed in {@link #open}. */
    public Compression getCompression() {
      return compression;
    }

    /**
     * Returns a {@link ReadableByteChannel} reading the data from this file, potentially
     * decompressing it using {@link #getCompression}.
     */
    public ReadableByteChannel open() throws IOException {
      return compression.readDecompressed(FileSystems.open(metadata.resourceId()));
    }

    /**
     * Returns a {@link SeekableByteChannel} equivalent to {@link #open}, but fails if this file is
     * not {@link MatchResult.Metadata#isReadSeekEfficient seekable}.
     */
    public SeekableByteChannel openSeekable() throws IOException {
      checkState(
          getMetadata().isReadSeekEfficient(),
          "The file %s is not seekable",
          metadata.resourceId());
      return ((SeekableByteChannel) open());
    }

    /** Returns the full contents of the file as bytes. */
    public byte[] readFullyAsBytes() throws IOException {
      return StreamUtils.getBytes(Channels.newInputStream(open()));
    }

    /** Returns the full contents of the file as a {@link String} decoded as UTF-8. */
    public String readFullyAsUTF8String() throws IOException {
      return new String(readFullyAsBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
      return "ReadableFile{metadata=" + metadata + ", compression=" + compression + '}';
    }
  }

  /**
   * Describes configuration for matching filepatterns, such as {@link EmptyMatchTreatment} and
   * continuous watching for matching files.
   */
  @AutoValue
  public abstract static class MatchConfiguration implements HasDisplayData, Serializable {
    /** Creates a {@link MatchConfiguration} with the given {@link EmptyMatchTreatment}. */
    public static MatchConfiguration create(EmptyMatchTreatment emptyMatchTreatment) {
      return new AutoValue_FileIO_MatchConfiguration.Builder()
          .setEmptyMatchTreatment(emptyMatchTreatment)
          .build();
    }

    abstract EmptyMatchTreatment getEmptyMatchTreatment();

    @Nullable
    abstract Duration getWatchInterval();

    @Nullable
    abstract TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setWatchInterval(Duration watchInterval);

      abstract Builder setWatchTerminationCondition(TerminationCondition<String, ?> condition);

      abstract MatchConfiguration build();
    }

    /** Sets the {@link EmptyMatchTreatment}. */
    public MatchConfiguration withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    /**
     * Continuously watches for new files at the given interval until the given termination
     * condition is reached, where the input to the condition is the filepattern.
     */
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition) {
      return toBuilder().setWatchInterval(interval).setWatchTerminationCondition(condition).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder
          .add(
              DisplayData.item("emptyMatchTreatment", getEmptyMatchTreatment().toString())
                  .withLabel("Treatment of filepatterns that match no files"))
          .addIfNotNull(
              DisplayData.item("watchForNewFilesInterval", getWatchInterval())
                  .withLabel("Interval to watch for new files"));
    }
  }

  /** Implementation of {@link #match}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract Match build();
    }

    /** Matches the given filepattern. */
    public Match filepattern(String filepattern) {
      return this.filepattern(StaticValueProvider.of(filepattern));
    }

    /** Like {@link #filepattern(String)} but using a {@link ValueProvider}. */
    public Match filepattern(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Match withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment(EmptyMatchTreatment)}. */
    public Match withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * See {@link MatchConfiguration#continuously}. The returned {@link PCollection} is unbounded.
     *
     * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via MatchAll", matchAll().withConfiguration(getConfiguration()));
    }
  }

  /** Implementation of {@link #matchAll}. */
  @AutoValue
  public abstract static class MatchAll
      extends PTransform<PCollection<String>, PCollection<MatchResult.Metadata>> {
    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract MatchAll build();
    }

    /** Like {@link Match#withConfiguration}. */
    public MatchAll withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** Like {@link Match#withEmptyMatchTreatment}. */
    public MatchAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Match#continuously}. */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input) {
      PCollection<MatchResult.Metadata> res;
      if (getConfiguration().getWatchInterval() == null) {
        res = input.apply(
            "Match filepatterns",
            ParDo.of(new MatchFn(getConfiguration().getEmptyMatchTreatment())));
      } else {
        res = input
            .apply(
                "Continuously match filepatterns",
                Watch.growthOf(new MatchPollFn(), Requirements.empty())
                    .withPollInterval(getConfiguration().getWatchInterval())
                    .withTerminationPerInput(getConfiguration().getWatchTerminationCondition()))
            .apply(Values.<MatchResult.Metadata>create());
      }
      return res.apply(Reshuffle.<MatchResult.Metadata>viaRandomKey());
    }

    private static class MatchFn extends DoFn<String, MatchResult.Metadata> {
      private final EmptyMatchTreatment emptyMatchTreatment;

      public MatchFn(EmptyMatchTreatment emptyMatchTreatment) {
        this.emptyMatchTreatment = emptyMatchTreatment;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        String filepattern = c.element();
        MatchResult match = FileSystems.match(filepattern, emptyMatchTreatment);
        LOG.info("Matched {} files for pattern {}", match.metadata().size(), filepattern);
        for (MatchResult.Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class MatchPollFn extends Watch.Growth.PollFn<String, MatchResult.Metadata> {
      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String element, Context c)
          throws Exception {
        return Watch.Growth.PollResult.incomplete(
            Instant.now(), FileSystems.match(element, EmptyMatchTreatment.ALLOW).metadata());
      }
    }
  }

  /** Implementation of {@link #readMatches}. */
  @AutoValue
  public abstract static class ReadMatches
      extends PTransform<PCollection<MatchResult.Metadata>, PCollection<ReadableFile>> {
    enum DirectoryTreatment {
      SKIP,
      PROHIBIT
    }

    abstract Compression getCompression();

    abstract DirectoryTreatment getDirectoryTreatment();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompression(Compression compression);

      abstract Builder setDirectoryTreatment(DirectoryTreatment directoryTreatment);

      abstract ReadMatches build();
    }

    /** Reads files using the given {@link Compression}. Default is {@link Compression#AUTO}. */
    public ReadMatches withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      return toBuilder().setCompression(compression).build();
    }

    /**
     * Controls how to handle directories in the input {@link PCollection}. Default is {@link
     * DirectoryTreatment#SKIP}.
     */
    public ReadMatches withDirectoryTreatment(DirectoryTreatment directoryTreatment) {
      checkArgument(directoryTreatment != null, "directoryTreatment can not be null");
      return toBuilder().setDirectoryTreatment(directoryTreatment).build();
    }

    @Override
    public PCollection<ReadableFile> expand(PCollection<MatchResult.Metadata> input) {
      return input.apply(ParDo.of(new ToReadableFileFn(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("compression", getCompression().toString()));
      builder.add(DisplayData.item("directoryTreatment", getDirectoryTreatment().toString()));
    }

    private static class ToReadableFileFn extends DoFn<MatchResult.Metadata, ReadableFile> {
      private final ReadMatches spec;

      private ToReadableFileFn(ReadMatches spec) {
        this.spec = spec;
      }

      @ProcessElement
      public void process(ProcessContext c) {
        MatchResult.Metadata metadata = c.element();
        if (metadata.resourceId().isDirectory()) {
          switch (spec.getDirectoryTreatment()) {
            case SKIP:
              return;

            case PROHIBIT:
              throw new IllegalArgumentException(
                  "Trying to read " + metadata.resourceId() + " which is a directory");

            default:
              throw new UnsupportedOperationException(
                  "Unknown DirectoryTreatment: " + spec.getDirectoryTreatment());
          }
        }

        Compression compression =
            (spec.getCompression() == Compression.AUTO)
                ? Compression.detect(metadata.resourceId().getFilename())
                : spec.getCompression();
        c.output(
            new ReadableFile(
                MatchResult.Metadata.builder()
                    .setResourceId(metadata.resourceId())
                    .setSizeBytes(metadata.sizeBytes())
                    .setIsReadSeekEfficient(
                        metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
                    .build(),
                compression));
      }
    }
  }

  /**
   * Specifies how to write elements to individual files in {@link FileIO#write} and {@link
   * FileIO#writeDynamic}.
   */
  public interface Sink<ElementT> extends Serializable {
    /** Initializes writing to the given channel. */
    void open(WritableByteChannel channel) throws IOException;

    /** Appends a single element to the file. */
    void write(ElementT element) throws IOException;

    /**
     * Flushes the buffered state (if any) before the channel is closed. Does not need to close the
     * channel.
     */
    void flush() throws IOException;

    String getDefaultMimeType();
  }

  /** Implementation of {@link #write} and {@link #writeDynamic}. */
  @AutoValue
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public abstract static class Write<DestinationT, UserT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
    /**
     * Information available for constructing a filename in {@link FilenamePolicy}. A {@link
     * FilenamePolicy} MUST return unique filenames for different combinations of {@link
     * #getNumShards}, {@link #getShardIndex}, {@link #getWindow}, {@link #getPane}.
     */
    interface FilenameContext {
      /** Current window being written. */
      BoundedWindow getWindow();

      /** Pane of the current window being written. */
      PaneInfo getPane();

      /** Total number of shards generated for the current window and pane. */
      int getNumShards();

      /**
       * Index of the current shard among all the shards generated for the current window and pane.
       */
      int getShardIndex();

      /** The specified {@link Compression}. */
      Compression getCompression();
    }

    /**
     * A policy for generating names for shard files using information in {@link FilenameContext}.
     */
    interface FilenamePolicy extends Serializable {
      /** Generates the filename. */
      ResourceId getFilename(FilenameContext context);
    }

    /**
     * Generates filenames by concatenating the given prefix, shard (according to the given shard
     * template), and suffix.
     *
     * <p>In the shard template:
     * <ul>
     *   <li>N stands for {@link FilenameContext#getNumShards()}. NNN stands for formatting it into
     *       3 digits, e.g. 042.
     *   <li>S stands for {@link FilenameContext#getShardIndex()}. SSS stands for formatting it into
     *       3 digits, e.g. 042.
     *   <li>W stands for {@link FilenameContext#getWindow()}
     *   <li>P stands for {@link FilenameContext#getPane()}
     * </ul>
     */
    public static FilenamePolicy nameFilesUsingShardTemplate(
        String prefix, String shardTemplate, String suffix) {
      return nameFilesUsingShardTemplate(StaticValueProvider.of(prefix), shardTemplate, suffix);
    }

    /**
     * Like {@link #nameFilesUsingShardTemplate(String, String, String)},
     * but with a {@link ValueProvider} for the prefix.
     */
    public static FilenamePolicy nameFilesUsingShardTemplate(
        final ValueProvider<String> prefix, final String shardTempate, final String suffix) {
      final DefaultFilenamePolicy policy =
          new DefaultFilenamePolicy(
              new DefaultFilenamePolicy.Params()
                  .withBaseFilename(toResource(prefix))
                  .withSuffix(suffix)
                  .withShardTemplate(shardTempate));
      return new FilenamePolicy() {
        @Override
        public ResourceId getFilename(final FilenameContext context) {
          return policy.windowedFilename(
              context.getShardIndex(),
              context.getNumShards(),
              context.getWindow(),
              context.getPane(),
              FileBasedSink.CompressionType.fromCanonical(context.getCompression()));
        }
      };
    }

    private static ValueProvider<ResourceId> toResource(ValueProvider<String> path) {
      return ValueProvider.NestedValueProvider.of(path,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          });
    }

    /**
     * Generates filenames as {@link #nameFilesUsingShardTemplate(String, String, String)} with a
     * shard template of {@link DefaultFilenamePolicy#DEFAULT_WINDOWED_SHARD_TEMPLATE}, which is
     * {@code W-P-SSSSS-of-NNNNN}.
     */
    public static FilenamePolicy nameFilesUsingWindowPaneAndShard(
        final String prefix, final String suffix) {
      return nameFilesUsingShardTemplate(
          prefix, DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, suffix);
    }

    /**
     * Like {@link #nameFilesUsingWindowPaneAndShard(String, String)}, but with a
     * {@link ValueProvider} for prefix.
     */
    public static FilenamePolicy nameFilesUsingWindowPaneAndShard(
        final ValueProvider<String> prefix, final String suffix) {
      return nameFilesUsingShardTemplate(
          prefix, DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, suffix);
    }

    /**
     * Generates filenames as {@link #nameFilesUsingShardTemplate(String, String, String)} with a
     * shard template of {@link DefaultFilenamePolicy#DEFAULT_UNWINDOWED_SHARD_TEMPLATE}, which is
     * {@code SSSSS-of-NNNNN} and ignores the window and pane.
     *
     * <p><b>Warning:</b>This policy can be used only if the input is globally windowed with a
     * default (or at-most-once) trigger.
     */
    public static FilenamePolicy nameFilesUsingOnlyShardIgnoringWindow(
        String prefix, String suffix) {
      return nameFilesUsingOnlyShardIgnoringWindow(StaticValueProvider.of(prefix), suffix);
    }

    /**
     * Like {@link #nameFilesUsingOnlyShardIgnoringWindow(String, String)}, but with a
     * {@link ValueProvider} for prefix.
     */
    public static FilenamePolicy nameFilesUsingOnlyShardIgnoringWindow(
      final ValueProvider<String> prefix, final String suffix) {
      final DefaultFilenamePolicy policy =
          new DefaultFilenamePolicy(
              new DefaultFilenamePolicy.Params()
                  .withBaseFilename(toResource(prefix))
                  .withSuffix(suffix)
                  .withShardTemplate(DEFAULT_UNWINDOWED_SHARD_TEMPLATE));
      return new FilenamePolicy() {
        @Override
        public ResourceId getFilename(final FilenameContext context) {
          checkArgument(
              context.getPane().getIndex() == 0 && context.getWindow() == GlobalWindow.INSTANCE,
              "nameFilesUsingOnlyShardIgnoringWindow supports writing only a single pane "
                  + "from the global window, but was applied to window %s pane %s",
              context.getWindow(),
              context.getPane());
          return policy.windowedFilename(
              context.getShardIndex(),
              context.getNumShards(),
              context.getWindow(),
              context.getPane(),
              FileBasedSink.CompressionType.fromCanonical(context.getCompression()));
        }
      };
    }

    @Nullable
    abstract Contextful<Fn<DestinationT, Sink<?>>> getSinkFn();

    @Nullable
    abstract Contextful<Fn<UserT, ?>> getOutputFn();

    @Nullable
    abstract Contextful<Fn<UserT, DestinationT>> getDestinationFn();

    @Nullable
    abstract Contextful<Fn<DestinationT, FilenamePolicy>> getFilenamePolicyFn();

    @Nullable
    abstract DestinationT getEmptyWindowDestination();

    @Nullable
    abstract Coder<DestinationT> getDestinationCoder();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectoryProvider();

    abstract Compression getCompression();

    @Nullable
    abstract ValueProvider<Integer> getNumShards();

    @Nullable
    abstract PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding();

    abstract boolean getIgnoreWindowing();

    abstract Builder<DestinationT, UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<DestinationT, UserT> {
      abstract Builder<DestinationT, UserT> setSinkFn(
          Contextful<Fn<DestinationT, Sink<?>>> sink);

      abstract Builder<DestinationT, UserT> setOutputFn(
          Contextful<Fn<UserT, ?>> outputFn);

      abstract Builder<DestinationT, UserT> setDestinationFn(
          Contextful<Fn<UserT, DestinationT>> destinationFn);

      abstract Builder<DestinationT, UserT> setFilenamePolicyFn(
          Contextful<Fn<DestinationT, FilenamePolicy>> policyFn);

      abstract Builder<DestinationT, UserT> setEmptyWindowDestination(
          DestinationT emptyWindowDestination);

      abstract Builder<DestinationT, UserT> setDestinationCoder(
          Coder<DestinationT> destinationCoder);

      abstract Builder<DestinationT, UserT> setTempDirectoryProvider(
          ValueProvider<ResourceId> tempDirectoryProvider);

      abstract Builder<DestinationT, UserT> setCompression(Compression compression);

      abstract Builder<DestinationT, UserT> setNumShards(ValueProvider<Integer> numShards);

      abstract Builder<DestinationT, UserT> setSharding(
          PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding);

      abstract Builder<DestinationT, UserT> setIgnoreWindowing(boolean ignoreWindowing);

      abstract Write<DestinationT, UserT> build();
    }

    /** Specifies how to partition elements into destinations. */
    public Write<DestinationT, UserT> by(
        Contextful<Fn<UserT, DestinationT>> destinationFn) {
      checkArgument(destinationFn != null, "destinationFn can not be null");
      return toBuilder().setDestinationFn(destinationFn).build();
    }

    /**
     * Specifies how create a {@link Sink} for a particular destination and how to map the element
     * type to the sink's output type.
     */
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn,
        Contextful<Fn<DestinationT, Sink<OutputT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return toBuilder()
        .setSinkFn((Contextful) sinkFn)
        .setOutputFn(outputFn).build();
    }

    /**
     * Like {@link #via(Contextful, Contextful)}, but uses the same sink for all
     * destinations.
     */
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn, Sink<OutputT> sink) {
      checkArgument(sink != null, "sink can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return via(
          outputFn,
          fn(SerializableFunctions.<DestinationT, Sink<OutputT>>constant(sink)));
    }

    /**
     * Like {@link #via(Contextful, Contextful)}, but the output type of the
     * sink is the same as the type of the input collection.
     */
    public Write<DestinationT, UserT> via(Contextful<Fn<DestinationT, Sink<UserT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      return toBuilder()
          .setSinkFn((Contextful) sinkFn)
          .setOutputFn(fn(SerializableFunctions.<UserT>identity()))
          .build();
    }

    /**
     * Like {@link #via(Contextful)}, but uses the same {@link Sink} for all destinations.
     */
    public Write<DestinationT, UserT> via(Sink<UserT> sink) {
      checkArgument(sink != null, "sink can not be null");
      return via(fn(SerializableFunctions.<DestinationT, Sink<UserT>>constant(sink)));
    }

    /** Specifies how to create a {@link FilenamePolicy} for a particular destination. */
    public Write<DestinationT, UserT> to(
        Contextful<Fn<DestinationT, FilenamePolicy>> policyFn) {
      checkArgument(policyFn != null, "policyFn can not be null");
      return toBuilder().setFilenamePolicyFn(policyFn).build();
    }

    /**
     * Like {@link #to(Contextful)} but uses the same {@link FilenamePolicy} for all
     * destinations.
     */
    public Write<DestinationT, UserT> to(FilenamePolicy policy) {
      checkArgument(policy != null, "policy can not be null");
      return to(fn(SerializableFunctions.<DestinationT, FilenamePolicy>constant(policy)));
    }

    /** Specifies a directory into which all temporary files will be placed. */
    public Write<DestinationT, UserT> withTempDirectory(
        ResourceId tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /** Like {@link #withTempDirectory(ValueProvider)}. */
    public Write<DestinationT, UserT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return toBuilder().setTempDirectoryProvider(tempDirectory).build();
    }

    /**
     * Specifies to compress all generated shard files using the given {@link Compression} and, by
     * default, append the respective extension to the filename.
     */
    public Write<DestinationT, UserT> withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      checkArgument(
          compression != Compression.AUTO, "AUTO compression is not supported for writing");
      return toBuilder().setCompression(compression).build();
    }

    /**
     * If {@link #withIgnoreWindowing()} is specified, specifies a destination to be used in case
     * the collection is empty, to generate the (only, empty) output file.
     */
    public Write<DestinationT, UserT> withEmptyGlobalWindowDestination(
        DestinationT emptyWindowDestination) {
      return toBuilder().setEmptyWindowDestination(emptyWindowDestination).build();
    }

    /**
     * Specifies a {@link Coder} for the destination type, if it can not be inferred from {@link
     * #by}.
     */
    public Write<DestinationT, UserT> withDestinationCoder(Coder<DestinationT> destinationCoder) {
      checkArgument(destinationCoder != null, "destinationCoder can not be null");
      return toBuilder().setDestinationCoder(destinationCoder).build();
    }

    /**
     * Specifies to use a given fixed number of shards per window. 0 means runner-determined
     * sharding. Specifying a non-zero value may hurt performance, because it will limit the
     * parallelism of writing and will introduce an extra {@link GroupByKey} operation.
     */
    public Write<DestinationT, UserT> withNumShards(int numShards) {
      checkArgument(numShards >= 0, "numShards must be non-negative, but was: %s", numShards);
      if (numShards == 0) {
        return withNumShards(null);
      }
      return withNumShards(StaticValueProvider.of(numShards));
    }

    /**
     * Like {@link #withNumShards(int)}. Specifying {@code null} means runner-determined sharding.
     */
    public Write<DestinationT, UserT> withNumShards(ValueProvider<Integer> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Specifies a {@link PTransform} to use for computing the desired number of shards in each
     * window.
     */
    public Write<DestinationT, UserT> withSharding(
        PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding) {
      checkArgument(sharding != null, "sharding can not be null");
      return toBuilder().setSharding(sharding).build();
    }

    /**
     * Specifies to ignore windowing information in the input, and instead rewindow it to global
     * window with the default trigger.
     *
     * @deprecated Avoid usage of this method: its effects are complex and it will be removed in
     *     future versions of Beam. Right now it exists for compatibility with {@link WriteFiles}.
     */
    @Deprecated
    public Write<DestinationT, UserT> withIgnoreWindowing() {
      return toBuilder().setIgnoreWindowing(true).build();
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      checkArgument(getSinkFn() != null, ".via() is required");
      checkArgument(getFilenamePolicyFn() != null, ".to() is required");
      checkArgument(getTempDirectoryProvider() != null, ".withTempDirectory() is required");
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder == null) {
        TypeDescriptor<DestinationT> destinationT =
            TypeDescriptors.outputOf(getDestinationFn().getClosure());
        try {
          destinationCoder =
              input
                  .getPipeline()
                  .getCoderRegistry()
                  .getCoder(destinationT);
        } catch (CannotProvideCoderException e) {
          throw new IllegalArgumentException(
              "Unable to infer a coder for destination type (inferred from .by() as \""
                  + destinationT
                  + "\") - specify it explicitly using .withDestinationCoder()");
        }
      }
      WriteFiles<UserT, DestinationT, ?> writeFiles =
          WriteFiles.to(new ViaFileBasedSink<>(this, destinationCoder))
              .withSideInputs(getAllSideInputs());
      if (getNumShards() != null) {
        writeFiles = writeFiles.withNumShards(getNumShards());
      } else if (getSharding() != null) {
        writeFiles = writeFiles.withSharding(getSharding());
      } else {
        writeFiles = writeFiles.withRunnerDeterminedSharding();
      }
      if (!getIgnoreWindowing()) {
        writeFiles = writeFiles.withWindowedWrites();
      }
      return input.apply(writeFiles);
    }

    private Collection<PCollectionView<?>> getAllSideInputs() {
      return Requirements.union(
              getDestinationFn(), getOutputFn(), getSinkFn(), getFilenamePolicyFn())
          .getSideInputs();
    }

    private static class WindowedFilenameContext implements FilenameContext {
      private final BoundedWindow window;
      private final PaneInfo paneInfo;
      private final int shardNumber;
      private final int numShards;
      private final Compression compression;

      public WindowedFilenameContext(
          BoundedWindow window,
          PaneInfo paneInfo,
          int shardNumber,
          int numShards,
          Compression compression) {
        this.window = window;
        this.paneInfo = paneInfo;
        this.shardNumber = shardNumber;
        this.numShards = numShards;
        this.compression = compression;
      }

      @Override
      public BoundedWindow getWindow() {
        return window;
      }

      @Override
      public PaneInfo getPane() {
        return paneInfo;
      }

      @Override
      public int getShardIndex() {
        return shardNumber;
      }

      @Override
      public int getNumShards() {
        return numShards;
      }

      @Override
      public Compression getCompression() {
        return compression;
      }
    }

    private static class ViaFileBasedSink<UserT, DestinationT, OutputT>
        extends FileBasedSink<UserT, DestinationT, OutputT> {
      private final Fn<DestinationT, Sink<OutputT>> sinkFn;

      private ViaFileBasedSink(
          Write<DestinationT, UserT> spec, Coder<DestinationT> destinationCoder) {
        super(new DynamicDestinationsAdapter<UserT, DestinationT, OutputT>(spec, destinationCoder));
        this.sinkFn = (Fn) spec.getSinkFn().getClosure();
      }

      @Override
      public Writer<OutputT> createWriter(final DestinationT destination) throws Exception {
        return new Writer<OutputT>() {
          Sink<OutputT> sink = sinkFn.apply(destination, new Fn.Context() {
            @Override
            public <T> T sideInput(PCollectionView<T> view) {
              return getDynamicDestinations().sideInput(view);
            }
          });

          @Override
          protected String getDefaultMimeType() {
            return sink.getDefaultMimeType();
          }

          @Override
          protected void prepareWrite(WritableByteChannel channel) throws Exception {
            sink.open(channel);
          }

          @Override
          public void write(OutputT value) throws Exception {
            sink.write(value);
          }

          @Override
          protected void finishWrite() throws Exception {
            sink.flush();
          }
        };
      }

      private static class DynamicDestinationsAdapter<UserT, DestinationT, OutputT>
          extends DynamicDestinations<UserT, DestinationT, OutputT> {
        private final Fn<UserT, DestinationT> destinationFn;
        private final Fn<UserT, OutputT> outputFn;
        private final Fn<DestinationT, Write.FilenamePolicy> filenamePolicyFn;
        private final DestinationT emptyWindowDestination;
        private final Coder<DestinationT> destinationCoder;
        private final Compression compression;
        private final Collection<PCollectionView<?>> sideInputs;
        private transient Fn.Context context;

        private DynamicDestinationsAdapter(
            Write<DestinationT, UserT> spec, Coder<DestinationT> destinationCoder) {
          this.destinationFn = spec.getDestinationFn().getClosure();
          this.outputFn = ((Fn<UserT, OutputT>) spec.getOutputFn().getClosure());
          this.filenamePolicyFn = spec.getFilenamePolicyFn().getClosure();
          this.emptyWindowDestination = spec.getEmptyWindowDestination();
          this.destinationCoder = destinationCoder;
          this.compression = spec.getCompression();
          this.sideInputs = spec.getAllSideInputs();
        }

        private Fn.Context getContext() {
          if (context == null) {
            context = new Fn.Context() {
              @Override
              public <T> T sideInput(PCollectionView<T> view) {
                return DynamicDestinationsAdapter.this.sideInput(view);
              }
            };
          }
          return context;
        }

        @Override
        public OutputT formatRecord(UserT record) {
          try {
            return outputFn.apply(record, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDestination(UserT element) {
          try {
            return destinationFn.apply(element, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDefaultDestination() {
          return emptyWindowDestination;
        }

        @Override
        public FilenamePolicy getFilenamePolicy(final DestinationT destination) {
          final Write.FilenamePolicy policyFn;
          try {
            policyFn = filenamePolicyFn.apply(destination, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return new FilenamePolicy() {
            @Override
            public ResourceId windowedFilename(
                int shardNumber,
                int numShards,
                BoundedWindow window,
                PaneInfo paneInfo,
                OutputFileHints outputFileHints) {
              // We ignore outputFileHints because it will always be the same as
              // spec.getCompression() because we control the FileBasedSink.
              return policyFn.getFilename(
                  new WindowedFilenameContext(
                      window, paneInfo, shardNumber, numShards, compression));
            }

            @Nullable
            @Override
            public ResourceId unwindowedFilename(
                int shardNumber, int numShards, OutputFileHints outputFileHints) {
              return policyFn.getFilename(
                  new WindowedFilenameContext(
                      GlobalWindow.INSTANCE,
                      PaneInfo.NO_FIRING,
                      shardNumber,
                      numShards,
                      compression));
            }
          };
        }

        @Override
        public List<PCollectionView<?>> getSideInputs() {
          return Lists.newArrayList(sideInputs);
        }

        @Nullable
        @Override
        public Coder<DestinationT> getDestinationCoder() {
          return destinationCoder;
        }
      }
    }
  }
}
