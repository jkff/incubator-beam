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
import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
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
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <h2>Reading Avro files</h2>
 *
 * <p>To read a {@link PCollection} from one or more Avro files with the same schema known at
 * pipeline construction time, use {@link #read}, using {@link AvroIO.Read#from} to specify the
 * filename or filepattern to read from. If the filepatterns to be read are themselves in a {@link
 * PCollection}, apply {@link #readAll}. If the schema is unknown at pipeline construction time, use
 * {@link #parseGenericRecords} or {@link #parseAllGenericRecords}.
 *
 * <p>Many configuration options below apply to several or all of these transforms.
 *
 * <p>See {@link FileSystems} for information on supported file systems and filepatterns.
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readAll}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * Read#withEmptyMatchTreatment} to configure this behavior.
 *
 * <p>By default, the filepatterns are expanded only once. {@link Read#watchForNewFiles} allows
 * streaming of new files matching the filepattern(s).
 *
 * <h3>Reading records of a known schema</h3>
 *
 * <p>To read specific records, such as Avro-generated classes, use {@link #read(Class)}. To read
 * {@link GenericRecord GenericRecords}, use {@link #readGenericRecords(Schema)} which takes a
 * {@link Schema} object, or {@link #readGenericRecords(String)} which takes an Avro schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema. Likewise, to read a {@link PCollection} of filepatterns, apply {@link
 * #readAllGenericRecords}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // Read Avro-generated classes from files on GCS
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.read(AvroAutoGenClass.class).from("gs://my_bucket/path/to/records-*.avro"));
 *
 * // Read GenericRecord's of the given schema from files on GCS
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.readGenericRecords(schema)
 *                .from("gs://my_bucket/path/to/records-*.avro"));
 * }</pre>
 *
 * <h3>Reading records of an unknown schema</h3>
 *
 * <p>To read records from files whose schema is unknown at pipeline construction time or differs
 * between files, use {@link #parseGenericRecords} - in this case, you will need to specify a
 * parsing function for converting each {@link GenericRecord} into a value of your custom type.
 * Likewise, to read a {@link PCollection} of filepatterns with unknown schema, use {@link
 * #parseAllGenericRecords}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Foo> records =
 *     p.apply(AvroIO.parseGenericRecords(new SerializableFunction<GenericRecord, Foo>() {
 *       public Foo apply(GenericRecord record) {
 *         // If needed, access the schema of the record using record.getSchema()
 *         return ...;
 *       }
 *     }));
 * }</pre>
 *
 * <h3>Reading from a {@link PCollection} of filepatterns</h3>
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> filepatterns = p.apply(...);
 * PCollection<AvroAutoGenClass> records =
 *     filepatterns.apply(AvroIO.read(AvroAutoGenClass.class));
 * PCollection<GenericRecord> genericRecords =
 *     filepatterns.apply(AvroIO.readGenericRecords(schema));
 * PCollection<Foo> records =
 *     filepatterns.apply(AvroIO.parseAllGenericRecords(new SerializableFunction...);
 * }</pre>
 *
 * <h3>Streaming new files matching a filepattern</h3>
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<AvroAutoGenClass> lines = p.apply(AvroIO
 *     .read(AvroAutoGenClass.class)
 *     .from("gs://my_bucket/path/to/records-*.avro")
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
 * <h2>Writing Avro files</h2>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use {@link AvroIO.Write}, using
 * {@code AvroIO.write().to(String)} to specify the output filename prefix. The default {@link
 * DefaultFilenamePolicy} will use this prefix, in conjunction with a {@link ShardNameTemplate} (set
 * via {@link Write#withShardNameTemplate(String)}) and optional filename suffix (set via {@link
 * Write#withSuffix(String)}, to generate output filenames in a sharded way. You can override this
 * default write filename policy using {@link Write#to(FileBasedSink.FilenamePolicy)} to specify a
 * custom file naming policy.
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the {@link
 * org.apache.avro.file.Codec CodecFactory.deflateCodec(6)}. This default can be changed or
 * overridden using {@link AvroIO.Write#withCodec}.
 *
 * <h3>Writing specific or generic records</h3>
 *
 * <p>To write specific records, such as Avro-generated classes, use {@link #write(Class)}. To write
 * {@link GenericRecord GenericRecords}, use either {@link #writeGenericRecords(Schema)} which takes
 * a {@link Schema} object, or {@link #writeGenericRecords(String)} which takes a schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records = ...;
 * records.apply(AvroIO.write(AvroAutoGenClass.class).to("/path/to/file.avro"));
 *
 * // A Write to a sharded GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply("WriteToAvro", AvroIO.writeGenericRecords(schema)
 *     .to("gs://my_bucket/path/to/numbers")
 *     .withSuffix(".avro"));
 * }</pre>
 *
 * <h3>Writing windowed or unbounded data</h3>
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner - {@link AvroIO.Write#withWindowedWrites()}
 * will cause windowing and triggering to be preserved. When producing windowed writes with a
 * streaming runner that supports triggers, the number of output shards must be set explicitly using
 * {@link AvroIO.Write#withNumShards(int)}; some runners may set this for you to a runner-chosen
 * value, so you may need not set it yourself. A {@link FileBasedSink.FilenamePolicy} must be set,
 * and unique windows and triggers must produce unique filenames.
 *
 * <h3>Writing data to multiple destinations</h3>
 *
 * <p>The following shows a more-complex example of AvroIO.Write usage, generating dynamic file
 * destinations as well as a dynamic Avro schema per file. In this example, a PCollection of user
 * events (e.g. actions on a website) is written out to Avro files. Each event contains the user id
 * as an integer field. We want events for each user to go into a specific directory for that user,
 * and each user's data should be written with a specific schema for that user; a side input is
 * used, so the schema can be calculated in a different stage.
 *
 * <pre>{@code
 * // This is the user class that controls dynamic destinations for this avro write. The input to
 * // AvroIO.Write will be UserEvent, and we will be writing GenericRecords to the file (in order
 * // to have dynamic schemas). Everything is per userid, so we define a dynamic destination type
 * // of Integer.
 * class UserDynamicAvroDestinations
 *     extends DynamicAvroDestinations<UserEvent, Integer, GenericRecord> {
 *   private final PCollectionView<Map<Integer, String>> userToSchemaMap;
 *   public UserDynamicAvroDestinations( PCollectionView<Map<Integer, String>> userToSchemaMap) {
 *     this.userToSchemaMap = userToSchemaMap;
 *   }
 *   public GenericRecord formatRecord(UserEvent record) {
 *     return formatUserRecord(record, getSchema(record.getUserId()));
 *   }
 *   public Schema getSchema(Integer userId) {
 *     return new Schema.Parser().parse(sideInput(userToSchemaMap).get(userId));
 *   }
 *   public Integer getDestination(UserEvent record) {
 *     return record.getUserId();
 *   }
 *   public Integer getDefaultDestination() {
 *     return 0;
 *   }
 *   public FilenamePolicy getFilenamePolicy(Integer userId) {
 *     return DefaultFilenamePolicy.fromParams(new Params().withBaseFilename(baseDir + "/user-"
 *     + userId + "/events"));
 *   }
 *   public List<PCollectionView<?>> getSideInputs() {
 *     return ImmutableList.<PCollectionView<?>>of(userToSchemaMap);
 *   }
 * }
 * PCollection<UserEvents> events = ...;
 * PCollectionView<Map<Integer, String>> userToSchemaMap = events.apply(
 *     "ComputePerUserSchemas", new ComputePerUserSchemas());
 * events.apply("WriteAvros", AvroIO.<Integer>writeCustomTypeToGenericRecords()
 *     .to(new UserDynamicAvroDestinations(userToSchemaMap)));
 * }</pre>
 */
public class AvroIO {
  /**
   * Reads records of the given type from an Avro file (or multiple Avro files matching a pattern).
   *
   * <p>The schema must be specified using one of the {@code withSchema} functions.
   */
  public static <T> Read<T> read(Class<T> recordClass) {
    return new AutoValue_AvroIO_Read.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .setHintMatchesManyFiles(false)
        .build();
  }

  /** Like {@link #read}, but reads each filepattern in the input {@link PCollection}. */
  public static <T> ReadAll<T> readAll(Class<T> recordClass) {
    return new AutoValue_AvroIO_ReadAll.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /** Reads Avro file(s) containing records of the specified schema. */
  public static Read<GenericRecord> readGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_Read.Builder<GenericRecord>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #readGenericRecords(Schema)}, but reads each filepattern in the input {@link
   * PCollection}.
   */
  public static ReadAll<GenericRecord> readAllGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_ReadAll.Builder<GenericRecord>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * Reads Avro file(s) containing records of the specified schema. The schema is specified as a
   * JSON-encoded string.
   */
  public static Read<GenericRecord> readGenericRecords(String schema) {
    return readGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Like {@link #readGenericRecords(String)}, but reads each filepattern in the input {@link
   * PCollection}.
   */
  public static ReadAll<GenericRecord> readAllGenericRecords(String schema) {
    return readAllGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Reads Avro file(s) containing records of an unspecified schema and converting each record to a
   * custom type.
   */
  public static <T> Parse<T> parseGenericRecords(SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_Parse.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setParseFn(parseFn)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each filepattern in the
   * input {@link PCollection}.
   */
  public static <T> ParseAll<T> parseAllGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_ParseAll.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setParseFn(parseFn)
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * Writes a {@link PCollection} to an Avro file (or multiple Avro files matching a sharding
   * pattern).
   */
  public static <T> Write<T> write(Class<T> recordClass) {
    return
        AvroIO.<T>defaultWriteBuilder()
            .setGenericRecords(false)
            .setSchema(ReflectData.get().getSchema(recordClass))
            .build();
  }

  /** Writes Avro records of the specified schema. */
  public static Write<GenericRecord> writeGenericRecords(Schema schema) {
    return
        AvroIO.<GenericRecord>defaultWriteBuilder()
            .setGenericRecords(true)
            .setSchema(schema)
            .build();
  }

  /**
   * Writes Avro records of the specified schema. The schema is specified as a JSON-encoded string.
   */
  public static Write<GenericRecord> writeGenericRecords(String schema) {
    return writeGenericRecords(new Schema.Parser().parse(schema));
  }

  private static <UserT> Write.Builder<UserT> defaultWriteBuilder() {
    return new AutoValue_AvroIO_Write.Builder<UserT>()
        .setFilenameSuffix("")
        .setShardTemplate(null)
        .setNumShards(0)
        .setCodec(Write.DEFAULT_SERIALIZABLE_CODEC)
        .setMetadata(ImmutableMap.<String, Object>of())
        .setWindowedWrites(false);
  }

  /** Implementation of {@link #read} and {@link #readGenericRecords}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract MatchConfiguration getMatchConfiguration();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();
    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);
      abstract Builder<T> setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Read<T> build();
    }

    /**
     * Reads from the given filename or filepattern.
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
     */
    public Read<T> from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read<T> from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }


    /** Sets the {@link MatchConfiguration}. */
    public Read<T> withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    /** Configures whether or not a filepattern matching no files is allowed. */
    public Read<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * Continuously watches for new files matching the filepattern, polling it at the given
     * interval, until the given termination condition is reached. The returned {@link PCollection}
     * is unbounded.
     *
     * <p>This works only in runners supporting {@link Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Read<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
              getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    /**
     * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
     * files.
     *
     * <p>This hint may cause a runner to execute the transform differently, in a way that improves
     * performance for this case, but it may worsen performance if the filepattern matches only a
     * small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
     * happen less efficiently within individual files).
     */
    public Read<T> withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "filepattern");
      checkNotNull(getSchema(), "schema");

      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply(
            "Read",
            org.apache.beam.sdk.io.Read.from(
                createSource(
                    getFilepattern(),
                    getMatchConfiguration().getEmptyMatchTreatment(),
                    getRecordClass(),
                    getSchema())));
      }
      // All other cases go through ReadAll.

      ReadAll<T> readAll =
          (getRecordClass() == GenericRecord.class)
              ? (ReadAll<T>) readAllGenericRecords(getSchema())
              : readAll(getRecordClass());
      readAll = readAll.withMatchConfiguration(getMatchConfiguration());
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via ReadAll", readAll);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .include("matchConfiguration", getMatchConfiguration());
    }

    @SuppressWarnings("unchecked")
    private static <T> AvroSource<T> createSource(
        ValueProvider<String> filepattern,
        EmptyMatchTreatment emptyMatchTreatment,
        Class<T> recordClass,
        Schema schema) {
      AvroSource<?> source =
          AvroSource.from(filepattern).withEmptyMatchTreatment(emptyMatchTreatment);
      return recordClass == GenericRecord.class
          ? (AvroSource<T>) source.withSchema(schema)
          : source.withSchema(recordClass);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    abstract MatchConfiguration getMatchConfiguration();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);
      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ReadAll<T> build();
    }


    /** Sets the {@link MatchConfiguration}. */
    public ReadAll<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public ReadAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public ReadAll<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    @VisibleForTesting
    ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      checkNotNull(getSchema(), "schema");
      return input
          .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(
              "Read all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  getDesiredBundleSizeBytes(),
                  new CreateSourceFn<>(getRecordClass(), getSchema().toString()),
                  AvroCoder.of(getRecordClass(), getSchema())));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include("matchConfiguration", getMatchConfiguration());
    }
  }

  private static class CreateSourceFn<T>
      implements SerializableFunction<String, FileBasedSource<T>> {
    private final Class<T> recordClass;
    private final Supplier<Schema> schemaSupplier;

    public CreateSourceFn(Class<T> recordClass, String jsonSchema) {
      this.recordClass = recordClass;
      this.schemaSupplier = AvroUtils.serializableSchemaSupplier(jsonSchema);
    }

    @Override
    public FileBasedSource<T> apply(String input) {
      return Read.createSource(
          StaticValueProvider.of(input),
          EmptyMatchTreatment.DISALLOW,
          recordClass,
          schemaSupplier.get());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseGenericRecords}. */
  @AutoValue
  public abstract static class Parse<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract MatchConfiguration getMatchConfiguration();
    abstract SerializableFunction<GenericRecord, T> getParseFn();
    @Nullable abstract Coder<T> getCoder();
    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setHintMatchesManyFiles(boolean hintMatchesManyFiles);

      abstract Parse<T> build();
    }

    /** Reads from the given filename or filepattern. */
    public Parse<T> from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }

    /** Like {@link #from(String)}. */
    public Parse<T> from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Parse<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public Parse<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Parse<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    /** Sets a coder for the result of the parse function. */
    public Parse<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /** Like {@link Read#withHintMatchesManyFiles()}. */
    public Parse<T> withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "filepattern");
      Coder<T> coder = inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());

      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply(
                org.apache.beam.sdk.io.Read.from(
                        AvroSource.from(getFilepattern()).withParseFn(getParseFn(), coder)));
      }
      // All other cases go through ParseAllGenericRecords.
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply(
              "Via ParseAll",
              parseAllGenericRecords(getParseFn())
                  .withCoder(coder)
                  .withMatchConfiguration(getMatchConfiguration()));
    }

    private static <T> Coder<T> inferCoder(
        @Nullable Coder<T> explicitCoder,
        SerializableFunction<GenericRecord, T> parseFn,
        CoderRegistry coderRegistry) {
      if (explicitCoder != null) {
        return explicitCoder;
      }
      // If a coder was not specified explicitly, infer it from parse fn.
      try {
        return coderRegistry.getCoder(TypeDescriptors.outputOf(parseFn));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn. Specify it explicitly using withCoder().",
            e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"))
          .include("matchConfiguration", getMatchConfiguration());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseAllGenericRecords}. */
  @AutoValue
  public abstract static class ParseAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    abstract MatchConfiguration getMatchConfiguration();
    abstract SerializableFunction<GenericRecord, T> getParseFn();
    @Nullable abstract Coder<T> getCoder();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ParseAll<T> build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public ParseAll<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public ParseAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public ParseAll<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
              getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    /** Specifies the coder for the result of the {@code parseFn}. */
    public ParseAll<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @VisibleForTesting
    ParseAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      final Coder<T> coder =
          Parse.inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());
      final SerializableFunction<GenericRecord, T> parseFn = getParseFn();
      final SerializableFunction<String, FileBasedSource<T>> createSource =
              new CreateParseSourceFn<>(parseFn, coder);
      return input
          .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(
              "Parse all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(getDesiredBundleSizeBytes(), createSource, coder));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"))
          .include("matchConfiguration", getMatchConfiguration());
    }

    private static class CreateParseSourceFn<T>
        implements SerializableFunction<String, FileBasedSource<T>> {
      private final SerializableFunction<GenericRecord, T> parseFn;
      private final Coder<T> coder;

      public CreateParseSourceFn(SerializableFunction<GenericRecord, T> parseFn, Coder<T> coder) {
        this.parseFn = parseFn;
        this.coder = coder;
      }

      @Override
      public FileBasedSource<T> apply(String input) {
        return AvroSource.from(input).withParseFn(parseFn, coder);
      }
    }
  }

  // ///////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<UserT> extends PTransform<PCollection<UserT>, PDone> {
    static final CodecFactory DEFAULT_CODEC = CodecFactory.deflateCodec(6);
    static final SerializableAvroCodecFactory DEFAULT_SERIALIZABLE_CODEC =
        new SerializableAvroCodecFactory(DEFAULT_CODEC);

    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();
    @Nullable abstract String getShardTemplate();
    abstract String getFilenameSuffix();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectory();

    abstract int getNumShards();

    abstract boolean getGenericRecords();

    @Nullable abstract Schema getSchema();
    abstract boolean getWindowedWrites();
    @Nullable abstract FilenamePolicy getFilenamePolicy();

    /**
     * The codec used to encode the blocks in the Avro file. String value drawn from those in
     * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
     */
    abstract SerializableAvroCodecFactory getCodec();
    /** Avro file metadata. */
    abstract ImmutableMap<String, Object> getMetadata();

    abstract Builder<UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT> {
      abstract Builder<UserT> setFilenamePrefix(
          ValueProvider<ResourceId> filenamePrefix);

      abstract Builder<UserT> setFilenameSuffix(String filenameSuffix);

      abstract Builder<UserT> setTempDirectory(
          ValueProvider<ResourceId> tempDirectory);

      abstract Builder<UserT> setNumShards(int numShards);

      abstract Builder<UserT> setShardTemplate(String shardTemplate);

      abstract Builder<UserT> setGenericRecords(boolean genericRecords);

      abstract Builder<UserT> setSchema(Schema schema);

      abstract Builder<UserT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT> setFilenamePolicy(
          FilenamePolicy filenamePolicy);

      abstract Builder<UserT> setCodec(SerializableAvroCodecFactory codec);

      abstract Builder<UserT> setMetadata(
          ImmutableMap<String, Object> metadata);

      abstract Write<UserT> build();
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
     * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
     * using {@link #to(FilenamePolicy)}.
     */
    public Write<UserT> to(String outputPrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems. This prefix is used by the {@link DefaultFilenamePolicy} to generate
     * filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
     * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
     * using {@link #to(FilenamePolicy)}.
     *
     * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
     * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
     * Custom filename policies do not automatically see this prefix - you should explicitly pass
     * the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<UserT> to(ResourceId outputPrefix) {
      return toResource(StaticValueProvider.of(outputPrefix));
    }

    private static class OutputPrefixToResourceId
        implements SerializableFunction<String, ResourceId> {
      @Override
      public ResourceId apply(String input) {
        return FileBasedSink.convertToFileResourceIfPossible(input);
      }
    }

    /** Like {@link #to(String)}. */
    public Write<UserT> to(ValueProvider<String> outputPrefix) {
      return toResource(
          NestedValueProvider.of(
              outputPrefix,
              // The function cannot be created as an anonymous class here since the enclosed class
              // may contain unserializable members.
              new OutputPrefixToResourceId()));
    }

    /** Like {@link #to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write<UserT> toResource(
        ValueProvider<ResourceId> outputPrefix) {
      return toBuilder().setFilenamePrefix(outputPrefix).build();
    }

    /**
     * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
     * directory for temporary files must be specified using {@link #withTempDirectory}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<UserT> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Sets the the output schema. Can only be used when the output type is {@link GenericRecord}.
     */
    public Write<UserT> withSchema(Schema schema) {
      return toBuilder().setSchema(schema).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public Write<UserT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public Write<UserT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<UserT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when using one
     * of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<UserT> withSuffix(String filenameSuffix) {
      checkArgument(filenameSuffix != null, "filenameSuffix can not be null");
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
    public Write<UserT> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template. This option is only compatible
     * with unwindowed writes.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write<UserT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public Write<UserT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    /** Writes to Avro file(s) compressed using specified codec. */
    public Write<UserT> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(new SerializableAvroCodecFactory(codec)).build();
    }

    /**
     * Writes to Avro file(s) with the specified metadata.
     *
     * <p>Supported value types are String, Long, and byte[].
     */
    public Write<UserT> withMetadata(Map<String, Object> metadata) {
      Map<String, String> badKeys = Maps.newLinkedHashMap();
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        Object v = entry.getValue();
        if (!(v instanceof String || v instanceof Long || v instanceof byte[])) {
          badKeys.put(entry.getKey(), v.getClass().getSimpleName());
        }
      }
      checkArgument(
          badKeys.isEmpty(),
          "Metadata value type must be one of String, Long, or byte[]. Found {}",
          badKeys);
      return toBuilder().setMetadata(ImmutableMap.copyOf(metadata)).build();
    }

    @Override
    public PDone expand(PCollection<UserT> input) {
      checkArgument(
          getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a AvroIO.Write "
              + "transform.");
      if (getFilenamePolicy() != null) {
        checkArgument(
            getShardTemplate() == null && getFilenameSuffix().isEmpty(),
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }

      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }

      AvroIO.Sink<UserT> sink;
      if (getGenericRecords()) {
        sink = sinkViaGenericRecords(getSchema(), new RecordFormatter<UserT>() {
          @Override
          public GenericRecord formatRecord(UserT element, Schema schema) {
            return (GenericRecord) element;
          }
        });
      } else {
        sink = sink((Class<UserT>) ReflectData.get().getClass(getSchema()));
      }
      sink = sink.withCodec(getCodec()).withMetadata(getMetadata());
      FileIO.Write<Void, UserT> write =
          FileIO.<UserT>write()
              .via(sink)
              .to(DefaultFilenamePolicy.toFileIOWriteFilenamePolicy(
                  getWindowedWrites(), getFilenamePolicy(), getFilenamePrefix(), getShardTemplate(),
                  getFilenameSuffix()))
              .withTempDirectory(tempDirectory);
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (!getWindowedWrites()) {
        write = write.withIgnoreWindowing();
      }
      input.apply("Write", write);
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
                  .withLabel("Directory for temporary files"));
    }

  }

  /////////////////////////////////////////////////////////////////////////////

  /** Converts an element of a custom type to a {@link GenericRecord} with the specified schema. */
  public abstract static class RecordFormatter<ElementT> implements Serializable {
    public abstract GenericRecord formatRecord(ElementT element, Schema schema);
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements of the given generated class, like {@link #write(Class)}.
   */
  public static <ElementT> Sink<ElementT> sink(final Class<ElementT> clazz) {
    return new AutoValue_AvroIO_Sink.Builder<ElementT>()
        .setJsonSchema(ReflectData.get().getSchema(clazz).toString())
        .setMetadata(ImmutableMap.<String, Object>of())
        .setCodec(Write.DEFAULT_SERIALIZABLE_CODEC)
        .build();
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements by converting each one to a {@link GenericRecord} with a given (common) schema.
   */
  public static <ElementT> Sink<ElementT> sinkViaGenericRecords(
      Schema schema, RecordFormatter<ElementT> formatter) {
    return new AutoValue_AvroIO_Sink.Builder<ElementT>()
        .setRecordFormatter(formatter)
        .setJsonSchema(schema.toString())
        .setMetadata(ImmutableMap.<String, Object>of())
        .setCodec(Write.DEFAULT_SERIALIZABLE_CODEC)
        .build();
  }

  /** Implementation of {@link #sink} and {@link #sinkViaGenericRecords}. */
  @AutoValue
  public abstract static class Sink<ElementT> implements FileIO.Sink<ElementT> {
    @Nullable abstract RecordFormatter<ElementT> getRecordFormatter();
    @Nullable abstract String getJsonSchema();
    abstract Map<String, Object> getMetadata();
    abstract SerializableAvroCodecFactory getCodec();

    abstract Builder<ElementT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ElementT> {
      abstract Builder<ElementT> setRecordFormatter(RecordFormatter<ElementT> formatter);
      abstract Builder<ElementT> setJsonSchema(String jsonSchema);
      abstract Builder<ElementT> setMetadata(Map<String, Object> metadata);
      abstract Builder<ElementT> setCodec(SerializableAvroCodecFactory codec);

      abstract Sink<ElementT> build();
    }

    /** Specifies to put the given metadata into each generated file. By default, empty. */
    public Sink<ElementT> withMetadata(Map<String, Object> metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    /**
     * Specifies to use the given {@link CodecFactory} for each generated file. By default, {@code
     * CodecFactory.deflateCodec(6)}.
     */
    public Sink<ElementT> withCodec(SerializableAvroCodecFactory codec) {
      return toBuilder().setCodec(codec).build();
    }

    private transient Schema schema;
    private transient DataFileWriter<ElementT> reflectWriter;
    private transient DataFileWriter<GenericRecord> genericWriter;

    @Override
    public String getDefaultMimeType() {
      return MimeTypes.BINARY;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.schema = new Schema.Parser().parse(getJsonSchema());
      DataFileWriter<?> writer;
      if (getRecordFormatter() == null) {
        writer = reflectWriter = new DataFileWriter<>(new ReflectDatumWriter<ElementT>(schema));
      } else {
        writer =
            genericWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
      }
      writer.setCodec(getCodec().getCodec());
      for (Map.Entry<String, Object> entry : getMetadata().entrySet()) {
        Object v = entry.getValue();
        if (v instanceof String) {
          writer.setMeta(entry.getKey(), (String) v);
        } else if (v instanceof Long) {
          writer.setMeta(entry.getKey(), (Long) v);
        } else if (v instanceof byte[]) {
          writer.setMeta(entry.getKey(), (byte[]) v);
        } else {
          throw new IllegalStateException(
              "Metadata value type must be one of String, Long, or byte[]. Found "
                  + v.getClass().getSimpleName());
        }
      }
      writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ElementT element) throws IOException {
      if (getRecordFormatter() == null) {
        reflectWriter.append(element);
      } else {
        genericWriter.append(getRecordFormatter().formatRecord(element, schema));
      }
    }

    @Override
    public void flush() throws IOException {
      MoreObjects.firstNonNull(reflectWriter, genericWriter).flush();
    }
  }

  /** Disallow construction of utility class. */
  private AvroIO() {}
}
