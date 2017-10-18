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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for XmlSink.
 */
@RunWith(JUnit4.class)
public class XmlSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * An XmlWriter correctly writes objects as Xml elements with an enclosing root element.
   */
  @Test
  public void testXmlWriter() throws Exception {
    XmlIO.Sink<Bird> sink =
            XmlIO.sink(Bird.class)
            .withRootElement("birds");

    List<Bird> bundle =
        Lists.newArrayList(new Bird("bemused", "robin"), new Bird("evasive", "goose"));
    List<String> lines = Arrays.asList("<birds>", "<bird>", "<species>robin</species>",
        "<adjective>bemused</adjective>", "</bird>", "<bird>", "<species>goose</species>",
        "<adjective>evasive</adjective>", "</bird>", "</birds>");
    runTestWrite(sink, bundle, lines, StandardCharsets.UTF_8.name());
  }

  @Test
  public void testXmlWriterCharset() throws Exception {
    XmlIO.Sink<Bird> sink =
        XmlIO.sink(Bird.class)
            .withRootElement("birds")
            .withCharset(StandardCharsets.ISO_8859_1.name());

    List<Bird> bundle = Lists.newArrayList(new Bird("bréche", "pinçon"));
    List<String> lines = Arrays.asList("<birds>", "<bird>", "<species>pinçon</species>",
        "<adjective>bréche</adjective>", "</bird>", "</birds>");
    runTestWrite(sink, bundle, lines, StandardCharsets.ISO_8859_1.name());
  }

  @Test
  public void testDisplayData() {
    XmlIO.Write<Integer> write = XmlIO.<Integer>write()
        .withRootElement("bird")
        .withRecordClass(Integer.class);

    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("rootElement", "bird"));
    assertThat(displayData, hasDisplayItem("recordClass", Integer.class));
  }

  /**
   * Write a bundle with an XmlWriter and verify the output is expected.
   */
  private <T> void runTestWrite(XmlIO.Sink<T> sink, List<T> bundle, List<String> expected,
                                String charset)
      throws Exception {
    File tmpFile = tmpFolder.newFile("foo.txt");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile)) {
      writeBundle(sink, bundle, fileOutputStream.getChannel());
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(tmpFile), charset))) {
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        line = line.trim();
        if (line.length() > 0) {
          lines.add(line);
        }
      }
      assertEquals(expected, lines);
    }
  }

  /**
   * Write a bundle with an XmlWriter.
   */
  private <T> void writeBundle(XmlIO.Sink<T> sink, List<T> elements, WritableByteChannel channel)
      throws Exception {
    sink.open(channel);
    for (T elem : elements) {
      sink.write(elem);
    }
    sink.flush();
  }

  /**
   * Test JAXB annotated class.
   */
  @SuppressWarnings("unused")
  @XmlRootElement(name = "bird")
  @XmlType(propOrder = {"name", "adjective"})
  private static final class Bird {
    private String name;
    private String adjective;

    @XmlElement(name = "species")
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getAdjective() {
      return adjective;
    }

    public void setAdjective(String adjective) {
      this.adjective = adjective;
    }

    public Bird() {}

    public Bird(String adjective, String name) {
      this.adjective = adjective;
      this.name = name;
    }
  }
}
