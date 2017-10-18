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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.values.TypeDescriptors.extractFromTypeParameters;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for file-based output. An implementation of FileBasedSink writes file-based output
 * and defines the format of output files (how values are written, headers/footers, MIME type,
 * etc.).
 *
 * <p>The process of writing to file-based sink is as follows:
 *
 * <ol>
 *   <li>An optional subclass-defined initialization,
 *   <li>a parallel write of bundles to temporary files, and finally,
 *   <li>these temporary files are renamed with final output filenames.
 * </ol>
 *
 * <p>In order to ensure fault-tolerance, a bundle may be executed multiple times (e.g., in the
 * event of failure/retry or for redundancy). However, exactly one of these executions will have its
 * result passed to the finalize method. Each call to {@link Writer#open} is passed a unique
 * <i>bundle id</i> when it is called by the WriteFiles transform, so even redundant or retried
 * bundles will have a unique way of identifying their output.
 *
 * <p>The bundle id should be used to guarantee that a bundle's output is unique. This uniqueness
 * guarantee is important; if a bundle is to be output to a file, for example, the name of the file
 * will encode the unique bundle id to avoid conflicts with other writers.
 *
 * <p>{@link FileBasedSink} can take a custom {@link FilenamePolicy} object to determine output
 * filenames, and this policy object can be used to write windowed or triggered PCollections into
 * separate files per window pane. This allows file output from unbounded PCollections, and also
 * works for bounded PCollecctions.
 *
 * <p>Supported file systems are those registered with {@link FileSystems}.
 *
 * <h2>Lifecycle of writing to a sink</h2>
 *
 * <p>The primary responsibilities of the WriteOperation is the management of output files. During a
 * write, {@link FileBasedSink.Writer}s write bundles to temporary file locations. After the bundles
 * have been written,
 *
 * <ol>
 *   <li>{@link #finalize} is given a list of the temporary files containing the
 *       output bundles.
 *   <li>During finalize, these temporary files are copied to final output locations and named
 *       according to a file naming template.
 *   <li>Finally, any temporary files that were created during the write are removed.
 * </ol>
 *
 * <p>Subclass implementations of WriteOperation must implement {@link #createWriter}
 * to return a concrete FileBasedSinkWriter.
 *
 * <h2>Temporary and Output File Naming:</h2>
 *
 * <p>During the write, bundles are written to temporary files using the tempDirectory that can be
 * provided via the constructor of WriteOperation. These temporary files will be named {@code
 * {tempDirectory}/{bundleId}}, where bundleId is the unique id of the bundle. For example, if
 * tempDirectory is "gs://my-bucket/my_temp_output", the output for a bundle with bundle id 15723
 * will be "gs://my-bucket/my_temp_output/15723".
 *
 * <p>Final output files are written to the location specified by the {@link FilenamePolicy}.
 * If no filename policy is specified, then the {@link DefaultFilenamePolicy} will be used.
 * The directory that the files are written to is determined by the {@link FilenamePolicy} instance.
 *
 * <p>Note that in the case of permanent failure of a bundle's write, no clean up of temporary files
 * will occur.
 *
 * <p>If there are no elements in the PCollection being written, no output will be generated.
 *
 * @param <OutputT> the type of values written to the sink.
 * @deprecated Use {@link FileIO#write} and {@link FileIO#writeDynamic} instead.
 */
@Experimental(Kind.FILESYSTEM)
@Deprecated
public abstract class FileBasedSink<UserT, DestinationT, OutputT>
    implements Serializable, HasDisplayData {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.class);

  /** @deprecated use {@link Compression}. */
  @Deprecated
  public enum CompressionType implements WritableByteChannelFactory {
    /** @see Compression#UNCOMPRESSED */
    UNCOMPRESSED(Compression.UNCOMPRESSED),

    /** @see Compression#GZIP */
    GZIP(Compression.GZIP),

    /** @see Compression#BZIP2 */
    BZIP2(Compression.BZIP2),

    /** @see Compression#DEFLATE */
    DEFLATE(Compression.DEFLATE);

    private Compression canonical;

    CompressionType(Compression canonical) {
      this.canonical = canonical;
    }

    @Override
    public String getSuggestedFilenameSuffix() {
      return canonical.getSuggestedSuffix();
    }

    @Override
    @Nullable
    public String getMimeType() {
      return (canonical == Compression.UNCOMPRESSED) ? null : MimeTypes.BINARY;
    }

    @Override
    public WritableByteChannel create(WritableByteChannel channel) throws IOException {
      return canonical.writeCompressed(channel);
    }

    public static CompressionType fromCanonical(Compression canonical) {
      switch(canonical) {
        case AUTO:
          throw new IllegalArgumentException("AUTO is not supported for writing");

        case UNCOMPRESSED:
          return UNCOMPRESSED;

        case GZIP:
          return GZIP;

        case BZIP2:
          return BZIP2;

        case ZIP:
          throw new IllegalArgumentException("ZIP is unsupported");

        case DEFLATE:
          return DEFLATE;

        default:
          throw new UnsupportedOperationException("Unsupported compression type: " + canonical);
      }
    }
  }

  /**
   * This is a helper function for turning a user-provided output filename prefix and converting it
   * into a {@link ResourceId} for writing output files. See {@link TextIO.Write#to(String)} for an
   * example use case.
   *
   * <p>Typically, the input prefix will be something like {@code /tmp/foo/bar}, and the user would
   * like output files to be named as {@code /tmp/foo/bar-0-of-3.txt}. Thus, this function tries to
   * interpret the provided string as a file {@link ResourceId} path.
   *
   * <p>However, this may fail, for example if the user gives a prefix that is a directory. E.g.,
   * {@code /}, {@code gs://my-bucket}, or {@code c://}. In that case, interpreting the string as a
   * file will fail and this function will return a directory {@link ResourceId} instead.
   */
  @Experimental(Kind.FILESYSTEM)
  public static ResourceId convertToFileResourceIfPossible(String outputPrefix) {
    try {
      return FileSystems.matchNewResource(outputPrefix, false /* isDirectory */);
    } catch (Exception e) {
      return FileSystems.matchNewResource(outputPrefix, true /* isDirectory */);
    }
  }

  private final DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations;

  /**
   * A class that allows value-dependent writes in {@link FileBasedSink}.
   *
   * <p>Users can define a custom type to represent destinations, and provide a mapping to turn this
   * destination type into an instance of {@link FilenamePolicy}.
   */
  @Experimental(Kind.FILESYSTEM)
  public abstract static class DynamicDestinations<UserT, DestinationT, OutputT>
      implements HasDisplayData, Serializable {
    interface SideInputAccessor {
      <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view);
    }

    private transient SideInputAccessor sideInputAccessor;

    static class SideInputAccessorViaProcessContext implements SideInputAccessor {
      private DoFn<?, ?>.ProcessContext processContext;

      SideInputAccessorViaProcessContext(DoFn<?, ?>.ProcessContext processContext) {
        this.processContext = processContext;
      }

      @Override
      public <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
        return processContext.sideInput(view);
      }
    }

    /**
     * Override to specify that this object needs access to one or more side inputs. This side
     * inputs must be globally windowed, as they will be accessed from the global window.
     */
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.of();
    }

    /**
     * Returns the value of a given side input. The view must be present in {@link
     * #getSideInputs()}.
     */
    protected final <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
      return sideInputAccessor.sideInput(view);
    }

    final void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
      this.sideInputAccessor = new SideInputAccessorViaProcessContext(context);
    }

    /** Convert an input record type into the output type. */
    public abstract OutputT formatRecord(UserT record);

    /**
     * Returns an object that represents at a high level the destination being written to. May not
     * return null. A destination must have deterministic hash and equality methods defined.
     */
    public abstract DestinationT getDestination(UserT element);

    /**
     * Returns the default destination. This is used for collections that have no elements as the
     * destination to write empty files to.
     */
    public abstract DestinationT getDefaultDestination();

    /**
     * Returns the coder for {@link DestinationT}. If this is not overridden, then the coder
     * registry will be use to find a suitable coder. This must be a deterministic coder, as {@link
     * DestinationT} will be used as a key type in a {@link
     * org.apache.beam.sdk.transforms.GroupByKey}.
     */
    @Nullable
    public Coder<DestinationT> getDestinationCoder() {
      return null;
    }

    /** Converts a destination into a {@link FilenamePolicy}. May not return null. */
    public abstract FilenamePolicy getFilenamePolicy(DestinationT destination);

    /** Populates the display data. */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {}

    // Gets the destination coder. If the user does not provide one, try to find one in the coder
    // registry. If no coder can be found, throws CannotProvideCoderException.
    final Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
        throws CannotProvideCoderException {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder != null) {
        return destinationCoder;
      }
      // If dynamicDestinations doesn't provide a coder, try to find it in the coder registry.
      @Nullable
      TypeDescriptor<DestinationT> descriptor =
          extractFromTypeParameters(
              this,
              DynamicDestinations.class,
              new TypeVariableExtractor<
                  DynamicDestinations<UserT, DestinationT, OutputT>, DestinationT>() {});
      try {
        return registry.getCoder(descriptor);
      } catch (CannotProvideCoderException e) {
        throw new CannotProvideCoderException(
            "Failed to infer coder for DestinationT from type "
                + descriptor + ", please provide it explicitly by overriding getDestinationCoder()",
            e);
      }
    }
  }

  /** A naming policy for output files. */
  @Experimental(Kind.FILESYSTEM)
  public abstract static class FilenamePolicy implements Serializable {
    /**
     * When a sink has requested windowed or triggered output, this method will be invoked to return
     * the file {@link ResourceId resource} to be created given the base output directory and a
     * {@link OutputFileHints} containing information about the file, including a suggested
     * extension (e.g. coming from {@link Compression}).
     *
     * <p>The policy must return unique and consistent filenames for different windows and panes.
     */
    @Experimental(Kind.FILESYSTEM)
    public abstract ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints);

    /**
     * When a sink has not requested windowed or triggered output, this method will be invoked to
     * return the file {@link ResourceId resource} to be created given the base output directory and
     * a {@link OutputFileHints} containing information about the file, including a suggested (e.g.
     * coming from {@link Compression}).
     *
     * <p>The shardNumber and numShards parameters, should be used by the policy to generate unique
     * and consistent filenames.
     */
    @Experimental(Kind.FILESYSTEM)
    @Nullable
    public abstract ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints);

    /** Populates the display data. */
    public void populateDisplayData(DisplayData.Builder builder) {}
  }

  /** The directory to which files will be written. */
  private final ValueProvider<ResourceId> tempDirectoryProvider;

  private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
    @Override
    public ResourceId apply(ResourceId input) {
      return input.getCurrentDirectory();
    }
  }

  /** Construct a {@link FileBasedSink} with the given temp directory and output channel type. */
  @Experimental(Kind.FILESYSTEM)
  public FileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations) {
    this.tempDirectoryProvider =
        NestedValueProvider.of(tempDirectoryProvider, new ExtractDirectory());
    this.dynamicDestinations = checkNotNull(dynamicDestinations);
  }

  /** Return the {@link DynamicDestinations} used. */
  @SuppressWarnings("unchecked")
  public DynamicDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicDestinations<UserT, DestinationT, OutputT>) dynamicDestinations;
  }

  /**
   * Returns the directory inside which temprary files will be written according to the configured
   * {@link FilenamePolicy}.
   */
  @Experimental(Kind.FILESYSTEM)
  public ValueProvider<ResourceId> getTempDirectoryProvider() {
    return tempDirectoryProvider;
  }

  public void validate(PipelineOptions options) {}

  /**
   * Clients must implement to return a subclass of {@link Writer}. This method must not mutate the
   * state of the object.
   */
  public abstract Writer<OutputT> createWriter(DestinationT destination)
      throws Exception;

  public void populateDisplayData(DisplayData.Builder builder) {
    getDynamicDestinations().populateDisplayData(builder);
  }

  /**
   * Abstract writer that writes a bundle to a {@link FileBasedSink}. Subclass implementations
   * provide a method that can write a single value to a {@link WritableByteChannel}.
   *
   * <p>Subclass implementations may also override methods that write headers and footers before and
   * after the values in a bundle, respectively, as well as provide a MIME type for the output
   * channel.
   *
   * <p>Multiple {@link Writer} instances may be created on the same worker, and therefore any
   * access to static members or methods should be thread safe.
   *
   * @param <OutputT> the type of values to write.
   */
  public abstract static class Writer<OutputT> {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    /** The output file for this bundle. May be null if opening failed. */
    private @Nullable ResourceId outputFile;
    private WritableByteChannel channel;

    /**
     * Called with the channel that a subclass will write its header, footer, and values to.
     * Subclasses should either keep a reference to the channel provided or create and keep a
     * reference to an appropriate object that they will use to write to it.
     *
     * <p>Called before any subsequent calls to writeHeader, writeFooter, and write.
     */
    protected abstract void prepareWrite(WritableByteChannel channel) throws Exception;

    /**
     * The MIME type used in the creation of the output channel (if the file system supports it).
     *
     * <p>This is the default for the sink, but it may be overridden by a supplied {@link
     * WritableByteChannelFactory}. For example, {@link TextIO.Write} uses {@link MimeTypes#TEXT} by
     * default but if {@link Compression#BZIP2} is set then the MIME type will be overridden to
     * {@link MimeTypes#BINARY}.
     */
    protected abstract String getDefaultMimeType();

    /** If any resources opened in the write processes need to be flushed, flush them here. */
    protected void finishWrite() throws Exception {}

    /** Initializes writing to the given file. */
    public final void open(ResourceId outputFile, WritableByteChannelFactory factory)
        throws Exception {
      LOG.debug("Opening writer to file {}", outputFile);
      this.outputFile = outputFile;

      // The factory may force a MIME type or it may return null, indicating to use the sink's MIME.
      String channelMimeType = firstNonNull(factory.getMimeType(), getDefaultMimeType());
      WritableByteChannel tempChannel = FileSystems.create(outputFile, channelMimeType);
      try {
        channel = factory.create(tempChannel);
      } catch (Exception e) {
        // If we have opened the underlying channel but fail to open the compression channel,
        // we should still close the underlying channel.
        closeChannelAndThrow(tempChannel, e);
      }

      // The caller shouldn't have to close() this Writer if it fails to open(), so close
      // the channel if prepareWrite() or writeHeader() fails.
      try {
        prepareWrite(channel);
      } catch (Exception e) {
        closeChannelAndThrow(channel, e);
      }
    }

    /** Called for each value in the bundle. */
    public abstract void write(OutputT value) throws Exception;

    public ResourceId getOutputFile() {
      return outputFile;
    }

    // Helper function to close a channel, on exception cases.
    // Always throws prior exception, with any new closing exception suppressed.
    private void closeChannelAndThrow(WritableByteChannel channel, Exception prior)
        throws Exception {
      try {
        channel.close();
      } catch (Exception e) {
        LOG.error("Closing channel for {} failed.", outputFile, e);
        prior.addSuppressed(e);
        throw prior;
      }
    }

    public final void deleteOutputFile() throws Exception {
      if (outputFile != null) {
        // outputFile may be null if open() was not called or failed.
        FileSystems.delete(
            Collections.singletonList(outputFile), StandardMoveOptions.IGNORE_MISSING_FILES);
      }
    }

    /** Closes the channel and returns the bundle result. */
    public final void close() throws Exception {
      checkState(outputFile != null, "FileResult.close cannot be called with a null outputFile");
      LOG.info("Closing {}", outputFile);

      try {
        finishWrite();
      } catch (Exception e) {
        closeChannelAndThrow(channel, outputFile, e);
      }

      LOG.debug("Closing channel to {}.", outputFile);
      try {
        channel.close();
      } catch (Exception e) {
        throw new IOException(String.format("Failed closing channel to %s", outputFile), e);
      }
    }
  }

  /**
   * Provides hints about how to generate output files, such as a suggested filename suffix (e.g.
   * based on the compression type), and the file MIME type.
   */
  public interface OutputFileHints extends Serializable {
    /**
     * Returns the MIME type that should be used for the files that will hold the output data. May
     * return {@code null} if this {@code WritableByteChannelFactory} does not meaningfully change
     * the MIME type (e.g., for {@link Compression#UNCOMPRESSED}).
     *
     * @see MimeTypes
     * @see <a href=
     *     'http://www.iana.org/assignments/media-types/media-types.xhtml'>http://www.iana.org/assignments/media-types/media-types.xhtml</a>
     */
    @Nullable
    String getMimeType();

    /**
     * @return an optional filename suffix, eg, ".gz" is returned for {@link Compression#GZIP}
     */
    @Nullable
    String getSuggestedFilenameSuffix();
  }

  /**
   * Implementations create instances of {@link WritableByteChannel} used by {@link FileBasedSink}
   * and related classes to allow <em>decorating</em>, or otherwise transforming, the raw data that
   * would normally be written directly to the {@link WritableByteChannel} passed into {@link
   * WritableByteChannelFactory#create(WritableByteChannel)}.
   *
   * <p>Subclasses should override {@link #toString()} with something meaningful, as it is used when
   * building {@link DisplayData}.
   */
  public interface WritableByteChannelFactory extends OutputFileHints {
    /**
     * @param channel the {@link WritableByteChannel} to wrap
     * @return the {@link WritableByteChannel} to be used during output
     */
    WritableByteChannel create(WritableByteChannel channel) throws IOException;
  }
}
