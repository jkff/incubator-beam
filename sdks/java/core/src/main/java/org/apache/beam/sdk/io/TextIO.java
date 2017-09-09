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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <h2>Reading text files</h2>
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a {@link
 * PCollection}, apply {@link TextIO#readAll()} or {@link TextIO#readFiles}.
 *
 * <p>{@link #read} returns a {@link PCollection} of {@link String Strings}, each corresponding to
 * one line of an input UTF-8 text file (split into lines delimited by '\n', '\r', or '\r\n', or
 * specified delimiter see {@link TextIO.Read#withDelimiter}).
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, the filepatterns are expanded only once. {@link Read#watchForNewFiles} and {@link
 * ReadAll#watchForNewFiles} allow streaming of new files matching the filepattern(s).
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readAll}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * TextIO.Read#withEmptyMatchTreatment} and {@link TextIO.ReadAll#withEmptyMatchTreatment} to
 * configure this behavior.
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>Example 2: reading a PCollection of filenames.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<String> lines = filenames.apply(TextIO.readAll());
 * }</pre>
 *
 * <p>Example 3: streaming new files matching a filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> lines = p.apply(TextIO.read()
 *     .from("/local/path/to/files/*")
 *     .watchForNewFiles(
 *       // Check for new files every minute
 *       Duration.standardMinutes(1),
 *       // Stop watching the filepattern if no new files appear within an hour
 *       afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Reading a very large number of files</h3>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link Read#withHintMatchesManyFiles} for better performance and
 * scalability. Note that it may decrease performance if the filepattern matches only a small number
 * of files.
 *
 * <h2>Writing text files</h2>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link TextIO.Write#to(String)} to specify the output prefix of the files to write.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"))
 *      .withSuffix(".txt")
 *      .withCompression(Compression.GZIP));
 * }</pre>
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 *
 * <p>If you want better control over how filenames are generated than the default policy allows, a
 * custom {@link FilenamePolicy} can also be set using {@link TextIO.Write#to(FilenamePolicy)}.
 *
 * <h3>Advanced features</h3>
 *
 * <p>{@link TextIO} supports all features of {@link FileIO#write} and {@link FileIO#writeDynamic},
 * such as writing windowed/unbounded data, writing data to multiple destinations, and so on, by
 * providing a {@link Sink} via {@link #sink()}.
 *
 * <p>{@link TextIO} provides an explicit shorthand for one of those features: {@link
 * Write#withWindowedWrites} is the opposite of {@link FileIO.Write#withIgnoreWindowing()}.
 *
 * <p>For example, to write events of different type to different filenames:
 *
 * <pre>{@code
 *   PCollection<Event> events = ...;
 *   events.apply(FileIO.<EventType, Event>writeDynamic()
 *         .by(Event::getType)
 *         .via(TextIO.sink(), Event::toString)
 *         .to(type -> nameFilesUsingWindowPaneAndShard(".../events/" + type + "/data", ".txt")));
 * }</pre>
 */
public class TextIO {
  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded
   * {@link PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_TextIO_Read.Builder()
        .setCompression(Compression.AUTO)
        .setHintMatchesManyFiles(false)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
   * PCollection} of filepatterns.
   *
   * <p>Can be applied to both bounded and unbounded {@link PCollection PCollections}, so this is
   * suitable for reading a {@link PCollection} of filepatterns arriving as a stream. However, every
   * filepattern is expanded once at the moment it is processed, rather than watched for new files
   * matching the filepattern to appear. Likewise, every file is read once, rather than watched for
   * new entries.
   */
  public static ReadAll readAll() {
    return new AutoValue_TextIO_ReadAll.Builder()
        .setCompression(Compression.AUTO)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  /**
   * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
   * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_TextIO_ReadFiles.Builder()
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   */
  public static Write write() {
    return new AutoValue_TextIO_Write.Builder()
        .setFilenamePrefix(null)
        .setTempDirectory(null)
        .setShardTemplate(null)
        .setFilenameSuffix("")
        .setFilenamePolicy(null)
        .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
        .setWindowedWrites(false)
        .setNumShards(0)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract MatchConfiguration getMatchConfiguration();
    abstract boolean getHintMatchesManyFiles();
    abstract Compression getCompression();
    @Nullable abstract byte[] getDelimiter();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);
      abstract Builder setCompression(Compression compression);
      abstract Builder setDelimiter(byte[] delimiter);

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
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
     */
    public Read from(String filepattern) {
      checkArgument(filepattern != null, "filepattern can not be null");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkArgument(filepattern != null, "filepattern can not be null");
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    /** @deprecated Use {@link #withCompression}. */
    @Deprecated
    public Read withCompressionType(TextIO.CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    /**
     * Reads from input sources using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
     */
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    /**
     * See {@link MatchConfiguration#continuously}.
     *
     * <p>This works only in runners supporting {@link Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Read watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    /**
     * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
     * files.
     *
     * <p>This hint may cause a runner to execute the transform differently, in a way that improves
     * performance for this case, but it may worsen performance if the filepattern matches only
     * a small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
     * happen less efficiently within individual files).
     */
    public Read withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment}. */
    public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n').
     */
    public Read withDelimiter(byte[] delimiter) {
      checkArgument(delimiter != null, "delimiter can not be null");
      checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
      return toBuilder().setDelimiter(delimiter).build();
    }

    static boolean isSelfOverlapping(byte[] s) {
      // s self-overlaps if v exists such as s = vu = wv with u and w non empty
      for (int i = 1; i < s.length - 1; ++i) {
        if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
          return true;
        }
      }
      return false;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
      }
      // All other cases go through ReadAll.
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply(
              "Via ReadAll",
              readAll()
                  .withCompression(getCompression())
                  .withMatchConfiguration(getMatchConfiguration())
                  .withDelimiter(getDelimiter()));
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<String> getSource() {
      return CompressedSource.from(
              new TextSource(
                  getFilepattern(),
                  getMatchConfiguration().getEmptyMatchTreatment(),
                  getDelimiter()))
          .withCompression(getCompression());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
          .include("matchConfiguration", getMatchConfiguration())
          .addIfNotNull(
              DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
              .withLabel("Custom delimiter to split records"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<String>> {
    abstract MatchConfiguration getMatchConfiguration();
    abstract Compression getCompression();
    @Nullable abstract byte[] getDelimiter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder setCompression(Compression compression);
      abstract Builder setDelimiter(byte[] delimiter);
      abstract ReadAll build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public ReadAll withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** @deprecated Use {@link #withCompression}. */
    @Deprecated
    public ReadAll withCompressionType(TextIO.CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    /**
     * Reads from input sources using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
     */
    public ReadAll withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    /** Same as {@link Read#withEmptyMatchTreatment}. */
    public ReadAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Same as {@link Read#watchForNewFiles(Duration, TerminationCondition)}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public ReadAll watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    ReadAll withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input
          .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              FileIO.readMatches()
                  .withCompression(getCompression())
                  .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(readFiles().withDelimiter(getDelimiter()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotNull(
              DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                  .withLabel("Custom delimiter to split records"))
          .include("matchConfiguration", getMatchConfiguration());
    }

  }

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>> {
    abstract long getDesiredBundleSizeBytes();
    @Nullable abstract byte[] getDelimiter();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);
      abstract Builder setDelimiter(byte[] delimiter);
      abstract ReadFiles build();
    }

    @VisibleForTesting
    ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    /** Like {@link Read#withDelimiter}. */
    public ReadFiles withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    @Override
    public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {
      return input.apply(
          "Read all via FileBasedSource",
          new ReadAllViaFileBasedSource<>(
              getDesiredBundleSizeBytes(),
              new CreateTextSourceFn(getDelimiter()),
              StringUtf8Coder.of()));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<String>> {
      private byte[] delimiter;

      private CreateTextSourceFn(byte[] delimiter) {
        this.delimiter = delimiter;
      }

      @Override
      public FileBasedSource<String> apply(String input) {
        return new TextSource(
            StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW, delimiter);
      }
    }
  }

  // ///////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {
    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();

    abstract String getFilenameSuffix();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectory();

    @Nullable abstract String getHeader();

    @Nullable abstract String getFooter();

    abstract int getNumShards();

    @Nullable abstract String getShardTemplate();

    @Nullable abstract FilenamePolicy getFilenamePolicy();

    abstract boolean getWindowedWrites();

    @Nullable
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    @Nullable
    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilenamePrefix(
          ValueProvider<ResourceId> filenamePrefix);

      abstract Builder setTempDirectory(
          ValueProvider<ResourceId> tempDirectory);

      abstract Builder setShardTemplate(@Nullable String shardTemplate);

      abstract Builder setFilenameSuffix(@Nullable String filenameSuffix);

      abstract Builder setHeader(@Nullable String header);

      abstract Builder setFooter(@Nullable String footer);

      abstract Builder setFilenamePolicy(
          @Nullable FilenamePolicy filenamePolicy);

      abstract Builder setNumShards(int numShards);

      abstract Builder setWindowedWrites(boolean windowedWrites);

      abstract Builder setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract Builder setCompression(Compression compression);

      abstract Write build();
    }

    /**
     * Writes to text files with the given prefix. The given {@code prefix} can reference any {@link
     * FileSystem} on the classpath. This prefix is used by the {@link DefaultFilenamePolicy} to
     * generate filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see {@link
     * #withNumShards(int)}), and a common suffix (if supplied using {@link #withSuffix(String)}).
     *
     * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
     * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
     * Custom filename policies do not automatically see this prefix - you should explicitly pass
     * the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    public Write to(String filenamePrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    }

    /** Like {@link #to(String)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write to(ResourceId filenamePrefix) {
      return toResource(StaticValueProvider.of(filenamePrefix));
    }

    /** Like {@link #to(String)}. */
    public Write to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /**
     * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
     * directory for temporary files must be specified using {@link #withTempDirectory}.
     */
    public Write to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /** Like {@link #to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public Write withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public Write withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides - i.e. not when using
     * {@link #to(FilenamePolicy)}.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when using one
     * of the default filename-prefix to() overrides - i.e. not when using {@link
     * #to(FilenamePolicy)}.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /**
     * Configures the number of output shards produced overall (when using unwindowed writes) or
     * per-window (when using windowed writes).
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system decide.
     */
    public Write withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Adds a header string to each file. A newline after the header is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured header.
     */
    public Write withHeader(@Nullable String header) {
      return toBuilder().setHeader(header).build();
    }

    /**
     * Adds a footer string to each file. A newline after the footer is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     */
    public Write withFooter(@Nullable String footer) {
      return toBuilder().setFooter(footer).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given {@link
     * WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output. The
     * default is value is {@link Compression#UNCOMPRESSED}.
     *
     * <p>A {@code null} value will reset the value to the default value mentioned above.
     *
     * @deprecated Use {@link #withCompression}.
     */
    @Deprecated
    public Write withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that compresses output using
     * the given {@link Compression}. The default value is {@link Compression#UNCOMPRESSED}.
     */
    public Write withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      return toBuilder().setCompression(compression).build();
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public Write withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      checkArgument(
          getFilenamePrefix() != null || getTempDirectory() != null,
          "Either filename prefix or .withTempDirectory() are required");
      checkArgument(
          getFilenamePolicy() == null || getFilenamePrefix() == null,
          "Filename policy and filename prefix are exclusive");
      checkArgument(
          getFilenamePolicy() != null || getFilenamePrefix() != null,
          ".to() is required");

      if (getFilenamePolicy() != null) {
        checkArgument(
            getShardTemplate() == null && getFilenameSuffix() == null,
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }
      checkArgument(
          getWritableByteChannelFactory() == null || getCompression() == null,
          "withWritableByteChannelFactory() and withCompression() are exclusive");

      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      Sink sink =
          getWritableByteChannelFactory() == null ? sink() : sink(getWritableByteChannelFactory());
      if (getHeader() != null) {
        sink = sink.withHeader(getHeader());
      }
      if (getFooter() != null) {
        sink = sink.withFooter(getFooter());
      }
      String filenameSuffix = "";
      if (!getFilenameSuffix().isEmpty()) {
        filenameSuffix = getFilenameSuffix();
      } else if (getWritableByteChannelFactory() != null) {
        filenameSuffix = getWritableByteChannelFactory().getSuggestedFilenameSuffix();
      } else if (getCompression() != null) {
        filenameSuffix = getCompression().getSuggestedSuffix();
      }
      FileIO.Write<Void, String> write =
          FileIO.<String>write()
              .via(sink)
              .to(DefaultFilenamePolicy.toFileIOWriteFilenamePolicy(
                  getWindowedWrites(), getFilenamePolicy(), getFilenamePrefix(), getShardTemplate(),
                  filenameSuffix))
              .withTempDirectory(tempDirectory);
      if (getCompression() != null) {
        write = write.withCompression(getCompression());
      }
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (!getWindowedWrites()) {
        write = write.withIgnoreWindowing();
      }
      input.apply("WriteFiles", write);
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .addIfNotDefault(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(
              DisplayData.item("tempDirectory", getTempDirectory())
                  .withLabel("Directory for temporary files"))
          .addIfNotNull(DisplayData.item("fileHeader", getHeader()).withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter()).withLabel("File Footer"));
      if (getCompression() != null) {
        builder.add(
            DisplayData.item("compression", getCompression().toString()).withLabel("Compression"));
      }
      if (getWritableByteChannelFactory() != null) {
        builder.add(
            DisplayData.item(
                    "writableByteChannelFactory", getWritableByteChannelFactory().toString())
                .withLabel("Compression/Transformation Type"));
      }
    }
  }

  /** @deprecated Use {@link Compression}. */
  @Deprecated
  public enum CompressionType {
    /** @see Compression#AUTO */
    AUTO(Compression.AUTO),

    /** @see Compression#UNCOMPRESSED */
    UNCOMPRESSED(Compression.UNCOMPRESSED),

    /** @see Compression#GZIP */
    GZIP(Compression.GZIP),

    /** @see Compression#BZIP2 */
    BZIP2(Compression.BZIP2),

    /** @see Compression#ZIP */
    ZIP(Compression.ZIP),

    /** @see Compression#ZIP */
    DEFLATE(Compression.DEFLATE);

    private Compression canonical;

    CompressionType(Compression canonical) {
      this.canonical = canonical;
    }

    /** @see Compression#matches */
    public boolean matches(String filename) {
      return canonical.matches(filename);
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a {@link Sink} that writes newline-delimited strings, for use with {@link
   * FileIO#write}.
   */
  public static Sink sink() {
    return sink(FileBasedSink.CompressionType.UNCOMPRESSED);
  }

  private static Sink sink(WritableByteChannelFactory writableByteChannelFactory) {
    return new AutoValue_TextIO_Sink.Builder()
        .setWritableByteChannelFactory(writableByteChannelFactory)
        .build();
  }

  /** Implementation of {@link #sink}. */
  @AutoValue
  public abstract static class Sink implements FileIO.Sink<String> {
    @Nullable abstract String getHeader();
    @Nullable abstract String getFooter();
    abstract WritableByteChannelFactory getWritableByteChannelFactory();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHeader(String header);
      abstract Builder setFooter(String footer);
      abstract Builder setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);
      abstract Sink build();
    }

    public Sink withHeader(String header) {
      checkArgument(header != null, "header can not be null");
      return toBuilder().setHeader(header).build();
    }

    public Sink withFooter(String footer) {
      checkArgument(footer != null, "footer can not be null");
      return toBuilder().setFooter(footer).build();
    }

    private transient WritableByteChannel channel;
    private transient PrintWriter writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.channel = getWritableByteChannelFactory().create(channel);
      writer = new PrintWriter(Channels.newOutputStream(this.channel));
      if (getHeader() != null) {
        writer.println(getHeader());
      }
    }

    @Override
    public void write(String element) throws IOException {
      writer.println(element);
    }

    @Override
    public void flush() throws IOException {
      if (getFooter() != null) {
        writer.println(getFooter());
      }
      writer.flush();
      this.channel.close();
    }
  }

  /** Disable construction of utility class. */
  private TextIO() {}
}
