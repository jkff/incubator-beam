/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.examples.bigshuffle;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.SDFTextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * An example of a large-scale group-by-key operation.
 *
 * <p>Given text files with one record per line, shuffles the data such that the records are grouped
 * by their first specified {@code keySize} bytes.
 */
public class BigShuffle {
  private static final int DEFAULT_RECORD_SIZE = 98;

  private static final int DEFAULT_KEY_SIZE = 10;

  static class ConvertStringToByteArray extends SimpleFunction<String, byte[]> {
    @Override
    public byte[] apply(String input) {
      return input.getBytes();
    }
  }

  /** Extracts the key (the first {@code keySize} bytes) and value (the rest) from the record. */
  static class ExtractKeyValueFn extends DoFn<byte[], KV<byte[], byte[]>> {
    private final int keySize;
    private final int elementSize;

    public ExtractKeyValueFn(int keySize, int elementSize) {
      this.keySize = keySize;
      this.elementSize = elementSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] record = c.element();

      // Catch the GCS data loss bugs
      // by checking that each record has the exact expected element size.
      Preconditions.checkArgument(
          record.length == elementSize,
          "Records must be of %s bytes, not %s",
          elementSize,
          record.length);

      c.output(
          KV.of(
              Arrays.copyOfRange(record, 0, keySize),
              Arrays.copyOfRange(record, keySize, record.length)));
    }
  }

  /** Groups the records by their first {@code keySize} bytes. */
  static class GroupByFirstNBytes extends PTransform<PCollection<byte[]>, PCollection<byte[]>> {
    private final int keySize;
    private final int elementSize;

    public GroupByFirstNBytes(int keySize, int elementSize) {
      Preconditions.checkArgument(
          elementSize > keySize,
          "Element size %s must be larger than key size %s",
          elementSize,
          keySize);
      this.keySize = keySize;
      this.elementSize = elementSize;
    }

    @Override
    public PCollection<byte[]> expand(PCollection<byte[]> lines) {
      return lines
          .apply("ExtractKeyValue", ParDo.of(new ExtractKeyValueFn(keySize, elementSize)))
          .apply(GroupByKey.<byte[], byte[]>create())
          .apply(
              "FormatOutput",
              ParDo.of(
                  new DoFn<KV<byte[], Iterable<byte[]>>, byte[]>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      byte[] key = c.element().getKey();
                      for (byte[] value : c.element().getValue()) {
                        byte[] out = new byte[key.length + value.length];
                        System.arraycopy(key, 0, out, 0, key.length);
                        System.arraycopy(value, 0, out, key.length, value.length);
                        c.output(out);
                      }
                    }
                  }));
    }
  }

  /**
   * Options supported by {@link BigShuffle}.
   *
   * <p>Inherits standard configuration options.
   */
  public static interface Options extends DataflowPipelineOptions {
    // TODO: Remove comment on output files when order is deterministic.
    @Description(
        "Path to the file to read from, or fake specification using "
            + "the 'fake' URI schema 'fake:numSeeds=<int>&numElementsPerSeed="
            + "<int>&elementSize=<int>[&seed=<long>]'. The optional seed parameter "
            + "allows BigShuffle to produce the same output when ran with the same "
            + "value. Note that the output files might not be in the same order.")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Key's size in terms of bytes")
    @Default.Integer(DEFAULT_KEY_SIZE)
    Integer getKeySize();
    void setKeySize(Integer keySize);

    @Description(
        "Expected checksum of the pipeline output. "
            + "Must be provided if --verifyChecksum is passed")
    @Validation.Required
    String getExpectedChecksum();
    void setExpectedChecksum(String value);
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    PCollection<byte[]> input;
    int elementSize;
    input =
        p.apply("ReadInput", SDFTextIO.read().from(options.getInput()))
            .apply(MapElements.via(new ConvertStringToByteArray()));
    elementSize = DEFAULT_RECORD_SIZE;

    // The BigShuffle branch.
    PCollection<byte[]> output =
        input.apply(new GroupByFirstNBytes(options.getKeySize(), elementSize));

    PAssert.thatSingleton(input.apply("InputChecksum", new Checksum()))
        .isEqualTo(options.getExpectedChecksum());

    PAssert.thatSingleton(output.apply("OutputChecksum", new Checksum()))
        .isEqualTo(options.getExpectedChecksum());

    p.run();
  }

  private static class Checksum extends PTransform<PCollection<byte[]>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<byte[]> values) {
      return values
          .apply(
              "ExtractChecksums",
              ParDo.of(
                  new DoFn<byte[], Long>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      byte[] record = c.element();
                      CRC32 crc32 = new CRC32();
                      crc32.update(record);
                      c.output(crc32.getValue());
                    }
                  }))
          .apply(Sum.longsGlobally().withoutDefaults())
          .apply(
              "FormatChecksums",
              ParDo.of(
                  new DoFn<Long, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      c.output(Long.toHexString(c.element()));
                    }
                  }));
    }
  }
}
