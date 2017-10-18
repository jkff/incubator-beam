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
package org.apache.beam.sdk.io.xml;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MimeTypes;

/** Implementation of {@link XmlIO#write}. */
class XmlSink<T> extends FileBasedSink<T, Void, T> {
  private static final String XML_EXTENSION = ".xml";

  private final XmlIO.Write<T> spec;

  private static <T> DefaultFilenamePolicy makeFilenamePolicy(XmlIO.Write<T> spec) {
    return DefaultFilenamePolicy.fromStandardParameters(
        spec.getFilenamePrefix(), ShardNameTemplate.INDEX_OF_MAX, XML_EXTENSION, false);
  }

  XmlSink(XmlIO.Write<T> spec) {
    super(spec.getFilenamePrefix(), DynamicFileDestinations.<T>constant(makeFilenamePolicy(spec)));
    this.spec = spec;
  }

  /**
   * Creates a {@link XmlWriter} with a marshaller for the type it will write.
   */
  @Override
  public XmlWriter<T> createWriter(Void dest) throws Exception {
    return new XmlWriter<>(this);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    spec.populateDisplayData(builder);
  }

  void populateFileBasedDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  /** A {@link Writer} that can write objects as XML elements. */
  protected static final class XmlWriter<T> extends Writer<T> {
    final Marshaller marshaller;
    private final String rootElement;
    private OutputStream os = null;

    public XmlWriter(XmlSink<T> sink) throws Exception {
      this.rootElement = sink.spec.getRootElement();

      JAXBContext context;
      context = JAXBContext.newInstance(sink.spec.getRecordClass());
      this.marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_ENCODING, sink.spec.getCharset());
    }

    @Override
    protected String getDefaultMimeType() {
      return MimeTypes.TEXT;
    }

    /**
     * Creates the output stream that elements will be written to.
     */
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      os = Channels.newOutputStream(channel);
      os.write(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "<" + rootElement + ">\n"));
    }

    /**
     * Writes a value to the stream.
     */
    @Override
    public void write(T value) throws Exception {
      marshaller.marshal(value, os);
    }

    /**
     * Writes the root element closing tag.
     */
    @Override
    protected void finishWrite() throws Exception {
      os.write(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "\n</" + rootElement + ">"));
      os.flush();
    }
  }
}
