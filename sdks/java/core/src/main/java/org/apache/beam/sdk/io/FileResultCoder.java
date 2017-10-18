package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/** A coder for {@link FileResult} objects. */
final class FileResultCoder<DestinationT> extends StructuredCoder<FileResult<DestinationT>> {
  private static final Coder<String> FILENAME_CODER = StringUtf8Coder.of();
  private static final Coder<Integer> SHARD_CODER = VarIntCoder.of();
  private static final Coder<PaneInfo> PANE_INFO_CODER =
      NullableCoder.of(PaneInfo.PaneInfoCoder.INSTANCE);
  private final Coder<BoundedWindow> windowCoder;
  private final Coder<DestinationT> destinationCoder;

  protected FileResultCoder(
      Coder<BoundedWindow> windowCoder, Coder<DestinationT> destinationCoder) {
    this.windowCoder = NullableCoder.of(windowCoder);
    this.destinationCoder = destinationCoder;
  }

  public static <DestinationT> FileResultCoder<DestinationT> of(
      Coder<BoundedWindow> windowCoder, Coder<DestinationT> destinationCoder) {
    return new FileResultCoder<>(windowCoder, destinationCoder);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(windowCoder);
  }

  @Override
  public void encode(FileResult<DestinationT> value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    FILENAME_CODER.encode(value.getTempFilename().toString(), outStream);
    windowCoder.encode(value.getWindow(), outStream);
    PANE_INFO_CODER.encode(value.getPaneInfo(), outStream);
    SHARD_CODER.encode(value.getShard(), outStream);
    destinationCoder.encode(value.getDestination(), outStream);
  }

  @Override
  public FileResult<DestinationT> decode(InputStream inStream) throws IOException {
    String tempFilename = FILENAME_CODER.decode(inStream);
    BoundedWindow window = windowCoder.decode(inStream);
    PaneInfo paneInfo = PANE_INFO_CODER.decode(inStream);
    int shard = SHARD_CODER.decode(inStream);
    DestinationT destination = destinationCoder.decode(inStream);
    return new FileResult<>(
        FileSystems.matchNewResource(tempFilename, false /* isDirectory */),
        shard,
        window,
        paneInfo,
        destination);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    FILENAME_CODER.verifyDeterministic();
    windowCoder.verifyDeterministic();
    PANE_INFO_CODER.verifyDeterministic();
    SHARD_CODER.verifyDeterministic();
    destinationCoder.verifyDeterministic();
  }
}
