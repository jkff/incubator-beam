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

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * A simple {@link FileBasedSink} that writes {@link String} values as lines with header and footer.
 */
class SimpleSink<DestinationT> extends FileBasedSink<String, DestinationT, String> {
  public SimpleSink(DynamicDestinations<String, DestinationT, String> dynamicDestinations) {
    super(dynamicDestinations);
  }

  public static SimpleSink<Void> makeSimpleSink(FilenamePolicy filenamePolicy) {
    return new SimpleSink<>(
        DynamicFileDestinations.<String>constant(filenamePolicy));
  }

  public static SimpleSink<Void> makeSimpleSink(
      ResourceId baseDirectory,
      String prefix,
      String shardTemplate,
      String suffix) {
    DynamicDestinations<String, Void, String> dynamicDestinations =
        DynamicFileDestinations.constant(
            DefaultFilenamePolicy.fromParams(
                new Params()
                    .withBaseFilename(
                        baseDirectory.resolve(prefix, StandardResolveOptions.RESOLVE_FILE))
                    .withShardTemplate(shardTemplate)
                    .withSuffix(suffix)));
    return new SimpleSink<>(dynamicDestinations);
  }

  @Override
  public SimpleWriter createWriter(DestinationT dest) throws Exception {
    return new SimpleWriter();
  }

  static final class SimpleWriter extends Writer<String> {
    static final String HEADER = "header";
    static final String FOOTER = "footer";

    private WritableByteChannel channel;

    @Override
    protected String getDefaultMimeType() {
      return MimeTypes.TEXT;
    }

    private static ByteBuffer wrap(String value) throws Exception {
      return ByteBuffer.wrap((value + "\n").getBytes("UTF-8"));
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      this.channel = channel;
      channel.write(wrap(HEADER));
    }

    @Override
    public void write(String value) throws Exception {
      channel.write(wrap(value));
    }

    @Override
    protected void finishWrite() throws Exception {
      channel.write(wrap(FOOTER));
    }
  }
}
