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

import static org.apache.beam.sdk.TestUtils.LINES2_ARRAY;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.io.FileIO.Write.nameFilesUsingOnlyShardIgnoringWindow;
import static org.apache.beam.sdk.io.FileIO.Write.nameFilesUsingShardTemplate;
import static org.apache.beam.sdk.io.ShardNameTemplate.INDEX_OF_MAX;
import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/** Tests for {@link TextIO.Write}. */
public class TextIOWriteTest implements Serializable {
  private static final String MY_HEADER = "myHeader";
  private static final String MY_FOOTER = "myFooter";

  private static Path tempFolder;

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setupClass() throws IOException {
    tempFolder = Files.createTempDirectory("TextIOTest");
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    Files.walkFileTree(
        tempFolder,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  class StartsWith implements Predicate<String> {
    String prefix;

    StartsWith(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public boolean apply(@Nullable String input) {
      return input.startsWith(prefix);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsViaSink() throws Exception {
    final ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tempFolder, "testDynamicDestinations").toString(), true);

    List<String> elements = Lists.newArrayList("aaaa", "aaab", "baaa", "baab", "caaa", "caab");
    PCollection<String> input = p.apply(Create.of(elements).withCoder(StringUtf8Coder.of()));
    input.apply(
        FileIO.<String, String>writeDynamic()
        .via(TextIO.sink())
        .by(fn(new SerializableFunction<String, String>() {
          @Override
          public String apply(String element) {
            // Destination is based on first character of string.
            return element.substring(0, 1);
          }
        }))
        .withDestinationCoder(StringUtf8Coder.of())
        .withEmptyGlobalWindowDestination("")
        .to(fn(new SerializableFunction<String, FileIO.Write.FilenamePolicy>() {
          @Override
          public FileIO.Write.FilenamePolicy apply(String destination) {
            return nameFilesUsingOnlyShardIgnoringWindow(
                baseDir.resolve(
                    "file_" + destination, ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                  .toString(),
                ".txt");
          }
        }))
        .withTempDirectory(baseDir));
    p.run();

    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("a")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_a", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString(),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
        ".txt");
    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("b")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_b", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString(),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
        ".txt");
    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("c")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_c", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString(),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
        ".txt");
  }

  private void runTestWrite(String[] elems) throws Exception {
    runTestWrite(elems, null, null, 1);
  }

  private void runTestWrite(String[] elems, int numShards) throws Exception {
    runTestWrite(elems, null, null, numShards);
  }

  private void runTestWrite(String[] elems, String header, String footer) throws Exception {
    runTestWrite(elems, header, footer, 1);
  }

  private static class MatchesFilesystem implements SerializableFunction<Iterable<String>, Void> {
    private final String baseFilename;

    MatchesFilesystem(String baseFilename) {
      this.baseFilename = baseFilename;
    }

    @Override
    public Void apply(Iterable<String> values) {
      try {
        String pattern = baseFilename + "*";
        List<String> matches = Lists.newArrayList();
        for (Metadata match :Iterables.getOnlyElement(
            FileSystems.match(Collections.singletonList(pattern))).metadata()) {
          matches.add(match.resourceId().toString());
        }
        assertThat(values, containsInAnyOrder(Iterables.toArray(matches, String.class)));
      } catch (Exception e) {
        fail("Exception caught " + e);
      }
      return null;
    }
  }

  private void runTestWrite(String[] elems, String header, String footer, int numShards)
      throws Exception {
    String outputName = "file";
    Path baseDir = Files.createTempDirectory(tempFolder, "testwrite");
    String baseFilename = baseDir.resolve(outputName).toString();

    PCollection<String> input =
        p.apply("CreateInput", Create.of(Arrays.asList(elems)).withCoder(StringUtf8Coder.of()));

    TextIO.Sink sink = TextIO.sink();
    if (header != null) {
      sink = sink.withHeader(header);
    }
    if (footer != null) {
      sink = sink.withFooter(footer);
    }
    FileIO.Write<Void, String> write = FileIO.<String>write()
        .via(sink)
        .withTempDirectory(
            FileSystems.matchNewResource(baseDir.toString(), true /* isDirectory */))
        .withIgnoreWindowing();
    if (numShards > 0) {
      write = write.withNumShards(numShards);
    }
    if (numShards == 1) {
      write = write.to(nameFilesUsingOnlyShardIgnoringWindow(baseFilename, ".txt"));
    } else {
      write = write.to(nameFilesUsingShardTemplate(baseFilename, INDEX_OF_MAX, ".txt"));
    }
    WriteFilesResult<Void> result = input.apply(write);

    PAssert.that(result.getPerDestinationOutputFilenames()
        .apply("GetFilenames", Values.<String>create()))
        .satisfies(new MatchesFilesystem(baseFilename));
    p.run();

    assertOutputFiles(
        elems,
        header,
        footer,
        numShards,
        baseFilename,
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
        ".txt");
  }

  private static void assertOutputFiles(
      String[] elems,
      final String header,
      final String footer,
      int numShards,
      String outputPrefix,
      String shardNameTemplate,
      String outputSuffix)
      throws Exception {
    List<File> expectedFiles = new ArrayList<>();
    if (numShards == 0) {
      String pattern = outputPrefix + "*";
      List<MatchResult> matches = FileSystems.match(Collections.singletonList(pattern));
      for (Metadata expectedFile : Iterables.getOnlyElement(matches).metadata()) {
        expectedFiles.add(new File(expectedFile.resourceId().toString()));
      }
    } else {
      for (int i = 0; i < numShards; i++) {
        expectedFiles.add(
            new File(
                DefaultFilenamePolicy.constructName(
                    FileSystems.matchNewResource(outputPrefix, false /* isDirectory */),
                    shardNameTemplate, outputSuffix, i, numShards, null, null)
                    .toString()));
      }
    }

    List<List<String>> actual = new ArrayList<>();

    for (File tmpFile : expectedFiles) {
      try (BufferedReader reader = new BufferedReader(new FileReader(tmpFile))) {
        List<String> currentFile = new ArrayList<>();
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          currentFile.add(line);
        }
        actual.add(currentFile);
      }
    }

    List<String> expectedElements = new ArrayList<>(elems.length);
    for (String elem : elems) {
      byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
      String line = new String(encodedElem);
      expectedElements.add(line);
    }

    List<String> actualElements =
        Lists.newArrayList(
            Iterables.concat(
                FluentIterable.from(actual)
                    .transform(removeHeaderAndFooter(header, footer))
                    .toList()));

    assertThat(actualElements, containsInAnyOrder(expectedElements.toArray()));

    assertTrue(Iterables.all(actual, haveProperHeaderAndFooter(header, footer)));
  }

  private static Function<List<String>, List<String>> removeHeaderAndFooter(
      final String header, final String footer) {
    return new Function<List<String>, List<String>>() {
      @Nullable
      @Override
      public List<String> apply(List<String> lines) {
        ArrayList<String> newLines = Lists.newArrayList(lines);
        if (header != null) {
          newLines.remove(0);
        }
        if (footer != null) {
          int last = newLines.size() - 1;
          newLines.remove(last);
        }
        return newLines;
      }
    };
  }

  private static Predicate<List<String>> haveProperHeaderAndFooter(
      final String header, final String footer) {
    return new Predicate<List<String>>() {
      @Override
      public boolean apply(List<String> fileLines) {
        int last = fileLines.size() - 1;
        return (header == null || fileLines.get(0).equals(header))
            && (footer == null || fileLines.get(last).equals(footer));
      }
    };
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteStrings() throws Exception {
    runTestWrite(LINES_ARRAY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStringsNoSharding() throws Exception {
    runTestWrite(NO_LINES_ARRAY, 0);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStrings() throws Exception {
    runTestWrite(NO_LINES_ARRAY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testShardedWrite() throws Exception {
    runTestWrite(LINES_ARRAY, 5);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithHeader() throws Exception {
    runTestWrite(LINES_ARRAY, MY_HEADER, null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithFooter() throws Exception {
    runTestWrite(LINES_ARRAY, null, MY_FOOTER);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithHeaderAndFooter() throws Exception {
    runTestWrite(LINES_ARRAY, MY_HEADER, MY_FOOTER);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithWritableByteChannelFactory() throws Exception {
    Coder<String> coder = StringUtf8Coder.of();
    String outputName = "file";
    ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tempFolder, "testwrite").toString(), true);

    PCollection<String> input = p.apply(Create.of(Arrays.asList(LINES2_ARRAY)).withCoder(coder));

    final WritableByteChannelFactory writableByteChannelFactory =
        new DrunkWritableByteChannelFactory();
    TextIO.Write write =
        TextIO.write()
            .to(
                baseDir
                    .resolve(outputName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                    .toString())
            .withoutSharding()
            .withWritableByteChannelFactory(writableByteChannelFactory);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("writableByteChannelFactory", "DRUNK"));

    input.apply(write);

    p.run();

    final List<String> drunkElems = new ArrayList<>(LINES2_ARRAY.length * 2 + 2);
    for (String elem : LINES2_ARRAY) {
      drunkElems.add(elem);
      drunkElems.add(elem);
    }
    assertOutputFiles(
        drunkElems.toArray(new String[0]),
        null,
        null,
        1,
        baseDir.resolve(
            outputName,
            ResolveOptions.StandardResolveOptions.RESOLVE_FILE).toString(),
        write.getShardTemplate(),
        ".drunk.txt");
  }

  @Test
  public void testWriteDisplayDataValidateThenHeader() {
    TextIO.Write write = TextIO.write().to("foo").withHeader("myHeader");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("fileHeader", "myHeader"));
  }

  @Test
  public void testWriteDisplayDataValidateThenFooter() {
    TextIO.Write write = TextIO.write().to("foo").withFooter("myFooter");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("fileFooter", "myFooter"));
  }

  @Test
  public void testGetName() {
    assertEquals("TextIO.Write", TextIO.write().to("somefile").getName());
  }

  /** Options for testing. */
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getOutput();
    void setOutput(ValueProvider<String> value);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApply() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);

    p.apply(Create.of("")).apply(TextIO.write().to(options.getOutput()));
  }
}
