package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.io.WriteFiles.UNKNOWN_SHARDNUM;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/**
 * Result of a single bundle write. Contains the filename produced by the bundle, and if known the
 * final output filename.
 */
final class FileResult<DestinationT> {
  private final ResourceId tempFilename;
  private final int shard;
  private final BoundedWindow window;
  private final PaneInfo paneInfo;
  private final DestinationT destination;

  @Experimental(Experimental.Kind.FILESYSTEM)
  public FileResult(
      ResourceId tempFilename,
      int shard,
      BoundedWindow window,
      PaneInfo paneInfo,
      DestinationT destination) {
    this.tempFilename = tempFilename;
    this.shard = shard;
    this.window = window;
    this.paneInfo = paneInfo;
    this.destination = destination;
  }

  @Experimental(Experimental.Kind.FILESYSTEM)
  public ResourceId getTempFilename() {
    return tempFilename;
  }

  public int getShard() {
    return shard;
  }

  public FileResult<DestinationT> withShard(int shard) {
    return new FileResult<>(tempFilename, shard, window, paneInfo, destination);
  }

  public BoundedWindow getWindow() {
    return window;
  }

  public PaneInfo getPaneInfo() {
    return paneInfo;
  }

  public DestinationT getDestination() {
    return destination;
  }

  @Experimental(Experimental.Kind.FILESYSTEM)
  public ResourceId getDestinationFile(
      FileBasedSink.DynamicDestinations<?, DestinationT, ?> dynamicDestinations,
      int numShards,
      FileBasedSink.OutputFileHints outputFileHints) {
    checkArgument(getShard() != UNKNOWN_SHARDNUM);
    checkArgument(numShards > 0);
    FileBasedSink.FilenamePolicy policy = dynamicDestinations.getFilenamePolicy(destination);
    if (getWindow() != null) {
      return policy.windowedFilename(
          getShard(), numShards, getWindow(), getPaneInfo(), outputFileHints);
    } else {
      return policy.unwindowedFilename(getShard(), numShards, outputFileHints);
    }
  }

  public String toString() {
    return MoreObjects.toStringHelper(FileResult.class)
        .add("tempFilename", tempFilename)
        .add("shard", shard)
        .add("window", window)
        .add("paneInfo", paneInfo)
        .toString();
  }
}
