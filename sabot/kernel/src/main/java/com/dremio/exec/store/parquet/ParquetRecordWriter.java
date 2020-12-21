/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.parquet;

import static com.dremio.common.arrow.DremioArrowSchema.DREMIO_ARROW_SCHEMA_2_1;
import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVectorHelper;
import org.apache.arrow.vector.complex.impl.SingleStructReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.NoExceptionAutoCloseables;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.factory.DefaultV1ValuesWriterFactory;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStoreExposer;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.ParquetOutputRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergCatalog;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

public class ParquetRecordWriter extends ParquetOutputRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordWriter.class);

  public enum Metric implements MetricDef {
    NUM_FILES_WRITTEN, // number of files written by the writer
    MIN_FILE_SIZE, // Minimum size of files written
    MAX_FILE_SIZE, // Maximum size of files written
    AVG_FILE_SIZE, // Average size of files written
    MIN_RECORD_COUNT_IN_FILE, // Minimum number of records written in a file
    MAX_RECORD_COUNT_IN_FILE, // Maximum number of records written in a file
    MIN_IO_WRITE_TIME, // Minimum IO write time
    MAX_IO_WRITE_TIME, // Maximum IO write time
    AVG_IO_WRITE_TIME, // Avg IO write time
    NUM_IO_WRITE,      // Total Number of IO writes
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  public static final String DRILL_VERSION_PROPERTY = "drill.version";
  public static final String DREMIO_VERSION_PROPERTY = "dremio.version";
  public static final String IS_DATE_CORRECT_PROPERTY = "is.date.correct";
  public static final String WRITER_VERSION_PROPERTY = "drill-writer.version";

  private final BufferAllocator codecAllocator;
  private final BufferAllocator columnEncoderAllocator;

  private final FileSystemPlugin<?> plugin;

  private ParquetFileWriter parquetFileWriter;
  private MessageType schema;
  private Map<String, String> extraMetaData = new HashMap<>();
  private int blockSize;
  private int pageSize;
  private boolean enableDictionary = false;
  private boolean enableDictionaryForBinary = false;
  private CompressionCodecName codec = CompressionCodecName.SNAPPY;
  private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;
  private CompressionCodecFactory codecFactory;
  private FileSystem fs;
  private Path path;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

  private ColumnWriteStore store;
  private PageWriteStore pageStore;

  private RecordConsumer consumer;
  private BatchSchema batchSchema;
  private UpdateTrackingConverter trackingConverter;

  private final String location;
  private final String prefix;
  private final String extension;
  private int index = 0;
  private final OperatorContext context;
  private WritePartition partition;
  private final int memoryThreshold;
  private final long maxPartitions;
  private final long minRecordsForFlush;
  private List<String> partitionColumns;
  private boolean isIcebergWriter;
  private CaseInsensitiveImmutableBiMap<Integer> icebergColumnIDMap;

  private final String queryUser;

  private final int parquetFileWriteTimeThresholdMilliSecs;
  private final double parquetFileWriteIoRateThresholdMbps;

  // metrics workspace variables
  int numFilesWritten = 0;
  long minFileSize = Long.MAX_VALUE;
  long maxFileSize = Long.MIN_VALUE;
  long avgFileSize = 0;
  long minRecordCountInFile = Long.MAX_VALUE;
  long maxRecordCountInFile = Long.MIN_VALUE;

  public ParquetRecordWriter(OperatorContext context, ParquetWriter writer, ParquetFormatConfig config) throws OutOfMemoryException{
    this.context = context;
    this.codecAllocator = context.getAllocator().newChildAllocator("ParquetCodecFactory", 0, Long.MAX_VALUE);
    this.columnEncoderAllocator = context.getAllocator().newChildAllocator("ParquetColEncoder", 0, Long.MAX_VALUE);
    this.codecFactory = CodecFactory.createDirectCodecFactory(new Configuration(),
        new ParquetDirectByteBufferAllocator(codecAllocator), pageSize);
    this.extraMetaData.put(DREMIO_VERSION_PROPERTY, DremioVersionInfo.getVersion());
    this.extraMetaData.put(IS_DATE_CORRECT_PROPERTY, "true");

    this.plugin = writer.getFormatPlugin().getFsPlugin();
    this.queryUser = writer.getProps().getUserName();

    FragmentHandle handle = context.getFragmentHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());

    this.location = writer.getLocation();
    this.prefix = fragmentId;
    this.extension = config.outputExtension;
    if (writer.getOptions() != null) {
      this.partitionColumns = writer.getOptions().getPartitionColumns();
      this.isIcebergWriter = (writer.getOptions().getIcebergWriterOperation() != WriterOptions.IcebergWriterOperation.NONE);
    } else {
      this.partitionColumns = null;
      this.isIcebergWriter = false;
    }

    if (this.isIcebergWriter && writer.getOptions().getExtendedProperty() != null) {
      initIcebergColumnIDList(writer.getOptions().getExtendedProperty());
    }

    memoryThreshold = (int) context.getOptions().getOption(ExecConstants.PARQUET_MEMORY_THRESHOLD_VALIDATOR);
    blockSize = (int) context.getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR);
    pageSize = (int) context.getOptions().getOption(ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR);
    final String codecName = context.getOptions().getOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR).toLowerCase();
    switch(codecName) {
    case "snappy":
      codec = CompressionCodecName.SNAPPY;
      break;
    case "lzo":
      codec = CompressionCodecName.LZO;
      break;
    case "gzip":
      codec = CompressionCodecName.GZIP;
      break;
    case "none":
    case "uncompressed":
      codec = CompressionCodecName.UNCOMPRESSED;
      break;
    default:
      throw new UnsupportedOperationException(String.format("Unknown compression type: %s", codecName));
    }

    enableDictionary = context.getOptions().getOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR);
    enableDictionaryForBinary = context.getOptions().getOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE_VALIDATOR);
    maxPartitions = context.getOptions().getOption(ExecConstants.PARQUET_MAXIMUM_PARTITIONS_VALIDATOR);
    minRecordsForFlush = context.getOptions().getOption(ExecConstants.PARQUET_MIN_RECORDS_FOR_FLUSH_VALIDATOR);
    parquetFileWriteTimeThresholdMilliSecs = (int)context.getOptions().getOption(ExecConstants.PARQUET_WRITE_TIME_THRESHOLD_MILLI_SECS_VALIDATOR);
    parquetFileWriteIoRateThresholdMbps = context.getOptions().getOption(ExecConstants.PARQUET_WRITE_IO_RATE_THRESHOLD_MBPS_VALIDATOR);
  }

  @Override
  public void setup() throws IOException {
    this.fs = plugin.createFS(queryUser, context);
    this.batchSchema = incoming.getSchema();
    if (this.isIcebergWriter && this.icebergColumnIDMap == null) {
      initIcebergColumnIDList();
    }
    newSchema();
  }

  private void initIcebergColumnIDList() {
    SchemaConverter schemaConverter = new SchemaConverter();
    org.apache.iceberg.Schema icebergSchema = schemaConverter.toIceberg(batchSchema);
    Map<String, Integer> schemaNameIDMap = IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    this.icebergColumnIDMap = CaseInsensitiveImmutableBiMap.newImmutableMap(schemaNameIDMap);
  }

  private void initIcebergColumnIDList(ByteString extendedProperty) {
    try {
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetXAttr.PARSER,
        extendedProperty.toByteArray());
      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = icebergDatasetXAttr.getColumnIdsList();
      Map<String, Integer> icebergColumns = new HashMap<>();
      icebergColumnIDs.forEach(field -> icebergColumns.put(field.getSchemaPath(), field.getId()));
      this.icebergColumnIDMap = CaseInsensitiveImmutableBiMap.newImmutableMap(icebergColumns);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not deserialize Parquet dataset info", e);
    }
  }

  /**
   * Helper method to create a new {@link ParquetFileWriter} as impersonated user.
   * @throws IOException
   */
  private void initRecordWriter() throws IOException {

    this.path = fs.canonicalizePath(partition.qualified(location, prefix + "_" + index + "." + extension));
    parquetFileWriter = new ParquetFileWriter(OutputFile.of(fs, path), checkNotNull(schema), ParquetFileWriter.Mode.CREATE, DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT, DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, true);
    parquetFileWriter.start();
  }

  private MessageType getParquetMessageTypeWithIds(BatchSchema batchSchema, String name) {
    List<Type> types = Lists.newArrayList();
    for (Field field : batchSchema) {
      if (field.getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
        continue;
      }
      if (field.getName().equalsIgnoreCase(IncrementalUpdateUtils.UPDATE_COLUMN)) {
        continue;
      }
      Type childType = getTypeWithId(field, field.getName());
      if (childType != null) {
        types.add(childType);
      }
    }
    Preconditions.checkState(types.size() > 0, "No types for parquet schema");
    return new MessageType(name, types);
  }

  private MessageType getParquetMessageType(BatchSchema batchSchema, String name) {
    if (isIcebergWriter) {
      return getParquetMessageTypeWithIds(batchSchema, name);
    }

    List<Type> types = Lists.newArrayList();
    for (Field field : batchSchema) {
      if (field.getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
        continue;
      }
      Type childType = getType(field);
      if (childType != null) {
        types.add(childType);
      }
    }
    Preconditions.checkState(types.size() > 0, "No types for parquet schema");
    return new MessageType(name, types);
  }

  private void newSchema() throws IOException {
    // Reset it to half of current number and bound it within the limits
    recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCountForNextMemCheck / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);

    String json = new Schema(batchSchema).toJson();
    extraMetaData.put(DREMIO_ARROW_SCHEMA_2_1, json);
    schema = getParquetMessageType(batchSchema, "root");

    int dictionarySize = (int)context.getOptions().getOption(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR);
    final ParquetProperties parquetProperties = ParquetProperties.builder()
      .withDictionaryPageSize(dictionarySize)
      .withWriterVersion(writerVersion)
      .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
      .withDictionaryEncoding(enableDictionary)
      .withAllocator(new ParquetDirectByteBufferAllocator(columnEncoderAllocator))
      .withPageSize(pageSize)
      .withAddPageHeadersToMetadata(true)
      .withEnableDictionarForBinaryType(enableDictionaryForBinary)
      .withPageRowCountLimit(Integer.MAX_VALUE) // Bug 16118
      .build();
    pageStore = ColumnChunkPageWriteStoreExposer.newColumnChunkPageWriteStore(
        toDeprecatedBytesCompressor(codecFactory.getCompressor(codec)), schema, parquetProperties);
    store = new ColumnWriteStoreV1(pageStore, parquetProperties);
    MessageColumnIO columnIO = new ColumnIOFactory(false).getColumnIO(this.schema);
    consumer = columnIO.getRecordWriter(store);
    setUp(schema, consumer);
  }

  private PrimitiveType getPrimitiveType(Field field) {
    MajorType majorType = getMajorTypeForField(field);
    MinorType minorType = majorType.getMinorType();
    String name = field.getName();
    PrimitiveTypeName primitiveTypeName = ParquetTypeHelper.getPrimitiveTypeNameForMinorType(minorType);
    if (primitiveTypeName == null) {
      return null;
    }
    OriginalType originalType = ParquetTypeHelper.getOriginalTypeForMinorType(minorType);
    int length = ParquetTypeHelper.getLengthForMinorType(minorType);
    DecimalMetadata decimalMetadata  = ParquetTypeHelper.getDecimalMetadataForField(majorType);
    return new PrimitiveType(OPTIONAL, primitiveTypeName, length, name, originalType, decimalMetadata, null);
  }

  @SuppressWarnings("deprecation")
  private static BytesCompressor toDeprecatedBytesCompressor(final BytesInputCompressor compressor) {
    return new BytesCompressor() {
      @Override
      public BytesInput compress(BytesInput bytes) throws IOException {
        return compressor.compress(bytes);
      }

      @Override
      public CompressionCodecName getCodecName() {
        return compressor.getCodecName();
      }

      @Override
      public void release() {
        compressor.release();
      }
    };
  }

  @Nullable
  private Type getType(Field field) {
    MinorType minorType = getMajorTypeForField(field).getMinorType();
    switch(minorType) {
      case STRUCT: {
        List<Type> types = Lists.newArrayList();
        for (Field childField : field.getChildren()) {
          Type childType = getType(childField);
          if (childType != null) {
            types.add(childType);
          }
        }
        if (types.size() == 0) {
          return null;
        }
        return new GroupType(OPTIONAL, field.getName(), types);
      }
      case LIST: {
        /**
         * We are going to build the following schema
         * <pre>
         * optional group <name> (LIST) {
         *   repeated group list {
         *     <element-repetition> <element-type> element;
         *   }
         * }
         * </pre>
         * see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">logical lists</a>
         */
        Field child = field.getChildren().get(0);
        Type childType = getType(child);
        if (childType == null) {
          return null;
        }
        childType = renameChildTypeToElement(getType(child));
        GroupType groupType = new GroupType(Repetition.REPEATED, "list", childType);
        return new GroupType(Repetition.OPTIONAL, field.getName(), OriginalType.LIST, groupType);
      }
      case UNION:
        List<Type> types = Lists.newArrayList();
        for (Field childField : field.getChildren()) {
          Type childType = getType(childField);
          if (childType != null) {
            types.add(childType);
          }
        }
        if (types.size() == 0) {
          return null;
        }
        return new GroupType(OPTIONAL, field.getName(), types);
      default:
        return getPrimitiveType(field);
    }
  }

  // Includes the column_id for this field and its children in the Parquet schema. The column_id is taken
  // from the equivalent field with name icebergFieldName from the icebergSchema
  //
  // icebergFieldName is the name of this field used by iceberg. Pass null to use the name from the field
  // icebergSchema contains the iceberg schema with column_ids. If non-null, this method extracts the column id
  //               for the field from iceberg schema and initializes the Parquet type accordingly
  @Nullable
  private Type getTypeWithId(Field field, String icebergFieldName) {
    MinorType minorType = getMajorTypeForField(field).getMinorType();
    int column_id = this.icebergColumnIDMap.get(icebergFieldName);
    switch(minorType) {
      case STRUCT: {
        List<Type> types = Lists.newArrayList();
        for (Field childField : field.getChildren()) {
          String childName = icebergFieldName + "." + childField.getName();
          Type childType = getTypeWithId(childField, childName);
          if (childType != null) {
            types.add(childType);
          }
        }
        if (types.size() == 0) {
          return null;
        }
        Type groupType = new GroupType(OPTIONAL, field.getName(), types);
        if (column_id != -1) {
          groupType = groupType.withId(column_id);
        }

        return groupType;
      }
      case LIST: {
        /**
         * We are going to build the following schema
         * <pre>
         * optional group <name> (LIST) {
         *   repeated group list {
         *     <element-repetition> <element-type> element;
         *   }
         * }
         * </pre>
         * see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">logical lists</a>
         */
        Field child = field.getChildren().get(0);

        // Dremio schema for list:
        // LIST:
        //   child = $data$ (single child)
        //      children defining the schema of the list elements
        // Iceberg schema for list is the same except that the single child's name is 'element' instead of '$data$'
        // Dremio renames the name to 'element' later by invoking renameChildTypeToElement()
        // Using 'element' as the field name since this is the name of the child node in the iceberg schema
        String childName = icebergFieldName + "." + "list.element";
        Type childType = getTypeWithId(child, childName);
        if (childType == null) {
          return null;
        }
        childType = renameChildTypeToElement(childType);
        GroupType groupType = new GroupType(Repetition.REPEATED, "list", childType);
        Type listType = new GroupType(Repetition.OPTIONAL, field.getName(), OriginalType.LIST, groupType);
        if (column_id != -1) {
          listType = listType.withId(column_id);
        }

        return listType;
      }
      case UNION:
        List<Type> types = Lists.newArrayList();
        for (Field childField : field.getChildren()) {
          String childName = icebergFieldName + "." + childField.getName();
          Type childType = getTypeWithId(childField, childName);
          if (childType != null) {
            types.add(childType);
          }
        }
        if (types.size() == 0) {
          return null;
        }
        return new GroupType(OPTIONAL, field.getName(), types);
    default:
        PrimitiveType primitiveType = getPrimitiveType(field);
        if (column_id != -1) {
          primitiveType = primitiveType.withId(column_id);
        }
        return primitiveType;
    }
  }

  /**
   * Changes the list inner '$data$' vector name to 'element' in the schema
   */
  private Type renameChildTypeToElement(Type childType) {
    if (childType.isPrimitive()) {
      PrimitiveType childPrimitiveType = childType.asPrimitiveType();
      return new PrimitiveType(childType.getRepetition(),
        childPrimitiveType.getPrimitiveTypeName(),
        childPrimitiveType.getTypeLength(),
        "element",
        childPrimitiveType.getOriginalType(),
        childPrimitiveType.getDecimalMetadata(),
        childPrimitiveType.getId());
    } else {
      GroupType childGroupType = childType.asGroupType();
      Type.ID id = childGroupType.getId();
      GroupType groupType = new GroupType(childType.getRepetition(),
        "element",
        childType.getOriginalType(),
        childGroupType.getFields());
      if (id != null) {
        groupType = groupType.withId(id.hashCode());
      }
      return groupType;
    }
  }


  private void flushAndClose() throws IOException {
    if(parquetFileWriter == null){
      return;
    }

    if (recordCount > 0) {
      long writeFileStartTimeMillis = System.currentTimeMillis();
      long memSize = store.getBufferedSize();
      parquetFileWriter.startBlock(recordCount);
      consumer.flush();
      store.flush();
      ColumnChunkPageWriteStoreExposer.flushPageStore(pageStore, parquetFileWriter);
      parquetFileWriter.endBlock();
      long recordsWritten = recordCount;

      long footerWriteAndFlushStartTimeMillis = System.currentTimeMillis();
      // we are writing one single block per file
      parquetFileWriter.end(extraMetaData);

      long writeFileEndTimeMillis = System.currentTimeMillis();

      logSlowIoWrite(writeFileStartTimeMillis, footerWriteAndFlushStartTimeMillis,  writeFileEndTimeMillis,
        parquetFileWriter.getPos(), recordsWritten, path);

      byte[] metadata = this.trackingConverter == null ? null : trackingConverter.getMetadata();
      final long fileSize = parquetFileWriter.getPos();
      listener.recordsWritten(recordsWritten, fileSize, path.toString(), metadata /** TODO: add parquet footer **/,
        partition.getBucketNumber(), getIcebergMetaData());
      parquetFileWriter = null;

      updateStats(memSize, recordCount);

      recordCount = 0;
    }

    if(store != null){
      store.close();
    }

    store = null;
    pageStore = null;
    index++;
  }

  private void logSlowIoWrite(long writeFileStartTimeMillis, long footerWriteAndFlushStartTimeMillis,
                             long writeFileEndTimeMillis, long size, long recordsWritten, Path path) {

    long writeBlockDeltaTime = footerWriteAndFlushStartTimeMillis - writeFileStartTimeMillis;
    long footerWriteAndFlushDeltaTime = writeFileEndTimeMillis - footerWriteAndFlushStartTimeMillis;
    long totalTime = writeBlockDeltaTime + footerWriteAndFlushDeltaTime;
    double writeIoRateMbps= Double.MAX_VALUE;
    if (totalTime > 0) {
      writeIoRateMbps = ((double)size / (1024 * 1024)) / ((double)totalTime / 1000);
    }

    if ((totalTime) > parquetFileWriteTimeThresholdMilliSecs && writeIoRateMbps < parquetFileWriteIoRateThresholdMbps) {
      logger.warn("DHL: ParquetFileWriter took too long (writeBlockDeltaTime {} and footerWriteAndFlushDeltaTime {} milli secs) " +
          "for writing {} records ({} bytes) to file {} at {} Mbps",
        writeBlockDeltaTime,footerWriteAndFlushDeltaTime, recordsWritten, size, path, String.format("%.3f",writeIoRateMbps));
    }
  }

  private byte[] getIcebergMetaData() throws IOException {
    if (!this.isIcebergWriter) {
      return null;
    }

    final long fileSize = parquetFileWriter.getPos();
    DataFiles.Builder dataFileBuilder =
      DataFiles.builder(IcebergCatalog.getIcebergPartitionSpec(this.batchSchema, this.partitionColumns))
        .withPath(path.toString())
        .withFileSizeInBytes(fileSize)
        .withRecordCount(recordCount)
        .withFormat(FileFormat.PARQUET);

    // add partition info
    if (partitionColumns != null) {
      dataFileBuilder = dataFileBuilder.withPartition(partition.getIcebergPartitionData());
    }

    // add column level metrics
    Metrics metrics = footerMetricsToIcebergMetrics(parquetFileWriter.getFooter(), batchSchema);
    dataFileBuilder = dataFileBuilder.withMetrics(metrics);
    return IcebergSerDe.serializeDataFile(dataFileBuilder.build());
  }

  public static Metrics footerMetricsToIcebergMetrics(ParquetMetadata metadata, BatchSchema batchSchema) {
    long rowCount = 0;
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Set<Integer> missingStats = Sets.newHashSet();

    org.apache.iceberg.Schema fileSchema = new SchemaConverter().toIceberg(batchSchema);

    List<BlockMetaData> blocks = metadata.getBlocks();
    for (BlockMetaData block : blocks) {
      rowCount += block.getRowCount();
      for (ColumnChunkMetaData column : block.getColumns()) {
        ColumnPath columnPath = column.getPath();
        NestedField field = fileSchema.findField(columnPath.toDotString());
        if (field == null) {
          // - lists are called favorite.list.element is parquet and just favorite.element in
          //   iceberg.
          // - dremio counts the only the top elements (i.e num lists), parquet counts the
          //   number of list elements (i.e total elements).
          // skipping this accounting for lists.
          continue;
        }

        int fieldId = field.fieldId();
        columnSizes.merge(fieldId, column.getTotalSize(), (x, y) -> y + column.getTotalSize());
        valueCounts.merge(fieldId, column.getValueCount(), (x, y) -> y + column.getValueCount());

        Statistics stats = column.getStatistics();
        if (stats == null) {
          missingStats.add(fieldId);
        } else if (!stats.isEmpty()) {
          nullValueCounts.merge(fieldId, stats.getNumNulls(), (x, y) -> y + stats.getNumNulls());
        }
      }
    }

    // discard accumulated values if any stats were missing
    for (Integer fieldId : missingStats) {
      nullValueCounts.remove(fieldId);
    }
    return new Metrics(rowCount, columnSizes, valueCounts, nullValueCounts);
  }

  private interface UpdateTrackingConverter {
    public byte[] getMetadata();
  }

  private static class UpdateIdTrackingConverter extends FieldConverter implements UpdateTrackingConverter {

    private UpdateIdWrapper updateIdWrapper;
    private final NullableTimeStampMilliHolder timeStampHolder = new NullableTimeStampMilliHolder();
    private final NullableDateMilliHolder dateHolder = new NullableDateMilliHolder();

    public UpdateIdTrackingConverter(int fieldId, String fieldName, FieldReader reader, com.dremio.common.types.MinorType type) {
      super(fieldId, fieldName, reader);
      this.updateIdWrapper = new UpdateIdWrapper(type);
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      switch(updateIdWrapper.getType()) {
        case FLOAT4:
          updateIdWrapper.update(reader.readFloat());
          break;
        case FLOAT8:
          updateIdWrapper.update(reader.readDouble());
          break;
        case VARCHAR:
          updateIdWrapper.update(reader.readText().toString());
          break;
        case TIMESTAMP:
          reader.read(timeStampHolder);
          updateIdWrapper.update(timeStampHolder.value, com.dremio.common.types.MinorType.TIMESTAMP);
          break;
        case DECIMAL:
          updateIdWrapper.update(reader.readBigDecimal());
          break;
        case INT:
          updateIdWrapper.update(reader.readInteger(), com.dremio.common.types.MinorType.INT);
          break;
        case BIGINT:
          updateIdWrapper.update(reader.readLong(), com.dremio.common.types.MinorType.BIGINT);
          break;
        case DATE:
          reader.read(dateHolder);
          int daysFromEpoch = (int) (dateHolder.value / 1000 / 60 / 60 / 24);
          updateIdWrapper.update(daysFromEpoch, com.dremio.common.types.MinorType.DATE);
          break;
        default:
      }
    }

    @Override
    public byte[] getMetadata() {
      if(updateIdWrapper.getUpdateId() != null) {
        return updateIdWrapper.serialize();
      }
      return null;
    }
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) { // bigint
    if(IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)){
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.BIGINT);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableBigIntConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTimeStampMilliConverter(int fieldId, String fieldName, FieldReader reader) { // timstamp
    if(IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)){
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.TIMESTAMP);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableTimeStampMilliConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) { // int
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.INT);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableIntConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) { // float
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.FLOAT4);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableFloat4Converter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) { // double
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.FLOAT8);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableFloat8Converter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableDecimalConverter(int fieldId, String fieldName, FieldReader reader) { // decimal
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.DECIMAL);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableDecimalConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) { // varchar
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.VARCHAR);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableVarCharConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableDateMilliConverter(int fieldId, String fieldName, FieldReader reader) { // date
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)) {
      UpdateIdTrackingConverter c = new UpdateIdTrackingConverter(fieldId, fieldName, reader, com.dremio.common.types.MinorType.DATE);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableDateMilliConverter(fieldId, fieldName, reader);
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
    if (index >= maxPartitions) {
      throw UserException.dataWriteError()
        .message("Materialization cancelled due to excessive partition creation. A single thread can only generate %d partitions. " +
          "Typically, this is a problem if you configure a partition or distribution column that has high cardinality. " +
          "If you want to increase this limit, you can change the \"store.max_partitions\" system option.", maxPartitions)
        .build(logger);
    }

    flushAndClose();
    this.partition = partition;
    newSchema();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck && recordCount >= minRecordsForFlush) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = store.getBufferedSize();
      if (context.getAllocator().getHeadroom() < memoryThreshold || memSize >= blockSize) {
        logger.debug("Reached block size " + blockSize);
        flushAndClose();
        newSchema();
      } else {
        // Find the average record size for encoded records so far
        float recordSize = ((float) memSize) / recordCount;

        final long recordsCouldFitInRemainingSpace = (long)((blockSize - memSize)/recordSize);

        // try to check again when reached half of the number of records that could potentially fit in remaining space.
        recordCountForNextMemCheck = recordCount +
            // Upper bound by the max count check. There is no lower bound, as it could cause files bigger than
            // blockSize if the remaining records that could fit is very few (usually when we are close to the goal).
            min(MAXIMUM_RECORD_COUNT_FOR_CHECK, recordsCouldFitInRemainingSpace/2);
      }
    }
  }

  @Override
  public FieldConverter getNewUnionConverter(int fieldId, String fieldName, FieldReader reader) {
    return new UnionParquetConverter(fieldId, fieldName, reader);
  }

  public class UnionParquetConverter extends ParquetFieldConverter {
    private UnionReader unionReader = null;
    Map<String, FieldConverter> converterMap = Maps.newHashMap();

    public UnionParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      unionReader = (UnionReader) reader;
      NonNullableStructVector internalMap = new UnionVectorHelper(unionReader.data)
        .getInternalMap();
      SingleStructReaderImpl mapReader = new SingleStructReaderImpl(internalMap);
      int i = 0;
      for (String name : mapReader) {
        FieldReader fieldReader = mapReader.reader(name);
        FieldConverter converter = EventBasedRecordWriter.getFieldConverter(ParquetRecordWriter
          .this, i, name, fieldReader.getMinorType(), unionReader);
        if (converter != null) {
          converterMap.put(name, converter);
          i++;
        }
      }
    }

    @Override
    public void writeValue() throws IOException {
      consumer.startGroup();
      int type = unionReader.data.getTypeValue(unionReader.getPosition());
      Types.MinorType minorType = Types.MinorType.values()[type];
      EventBasedRecordWriter.FieldConverter converter = converterMap.get(minorType.name().toLowerCase());
      if(converter != null) {
        converter.writeField();
      }
      consumer.endGroup();
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      consumer.startField(fieldName, fieldId);
      writeValue();
      consumer.endField(fieldName, fieldId);
    }
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    MapParquetConverter converter = new MapParquetConverter(fieldId, fieldName, reader);
    if (converter.converters.size() == 0) {
      return null;
    }
    return converter;
  }

  public class MapParquetConverter extends ParquetFieldConverter {
    List<FieldConverter> converters = Lists.newArrayList();

    public MapParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      int i = 0;
      for (String name : reader) {
        FieldReader fieldReader = reader.reader(name);
        FieldConverter converter = EventBasedRecordWriter.getConverter(ParquetRecordWriter.this, i, name,
            fieldReader.getMinorType(), fieldReader);
        if (converter != null) {
          converters.add(converter);
          i++;
        }
      }
    }

    @Override
    public void writeValue() throws IOException {
      consumer.startGroup();
      for (FieldConverter converter : converters) {
        converter.writeField();
      }
      consumer.endGroup();
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      consumer.startField(fieldName, fieldId);
      writeValue();
      consumer.endField(fieldName, fieldId);
    }
  }

  @Override
  public FieldConverter getNewListConverter(int fieldId, String fieldName, FieldReader reader) {
    if (reader.getField().getChildren().get(0).getFieldType().getType().equals(Null.INSTANCE)) {
      return null;
    }
    return new ListParquetConverter(fieldId, fieldName, reader);
  }

  public class ListParquetConverter extends ParquetFieldConverter {
    ParquetFieldConverter innerConverter;

    public ListParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      int i = 0;
      FieldReader fieldReader = reader.reader();
      innerConverter = (ParquetFieldConverter) EventBasedRecordWriter.getConverter(ParquetRecordWriter.this, i++,
          "element", fieldReader.getMinorType(), fieldReader);
    }

    @Override
    public void writeValue() throws IOException {
      throw new UnsupportedOperationException("List of list not supported in ParquetWriter");
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return; // null field
      }
      consumer.startField(fieldName, fieldId);
      consumer.startGroup(); // field group

      // without this check we get the following exception when the list is empty:
      // ParquetEncodingException: empty fields are illegal, the field should be omitted completely instead
      if (reader.size() != 0) {

        consumer.startField("list", 0);
        while (reader.next()) {
          consumer.startGroup(); // list group
          innerConverter.writeField(); // element
          consumer.endGroup();
        }
        consumer.endField("list", 0);

      }

      consumer.endGroup();
      consumer.endField(fieldName, fieldId);
    }
  }


  @Override
  public void startRecord() throws IOException {
    consumer.startMessage();
  }

  @Override
  public void endRecord() throws IOException {
    consumer.endMessage();

    // we wait until there is at least one record before creating the parquet file
    if (parquetFileWriter == null) {
      initRecordWriter();
    }

    recordCount++;

    checkBlockSizeReached();
  }

  @Override
  public void abort() throws IOException {
  }

  private void updateStats(long memSize, long recordCount) {
    minFileSize = min(minFileSize, memSize);
    maxFileSize = max(maxFileSize, memSize);
    avgFileSize = (avgFileSize * numFilesWritten + memSize) / (numFilesWritten + 1);
    minRecordCountInFile = min(minRecordCountInFile, recordCount);
    maxRecordCountInFile = max(maxRecordCountInFile, recordCount);
    numFilesWritten++;

    final OperatorStats stats = context.getStats();
    stats.setLongStat(Metric.NUM_FILES_WRITTEN, numFilesWritten);
    stats.setLongStat(Metric.MIN_FILE_SIZE, minFileSize);
    stats.setLongStat(Metric.MAX_FILE_SIZE, maxFileSize);
    stats.setLongStat(Metric.AVG_FILE_SIZE, avgFileSize);
    stats.setLongStat(Metric.MIN_RECORD_COUNT_IN_FILE, minRecordCountInFile);
    stats.setLongStat(Metric.MAX_RECORD_COUNT_IN_FILE, maxRecordCountInFile);
  }

  @Override
  public void close() throws Exception {
    try {
      flushAndClose();
      OperatorStats operatorStats = context.getStats();
      OperatorStats.IOStats ioStats = operatorStats.getWriteIOStats();

      if (ioStats != null) {
        long minIOWriteTime = ioStats.minIOTime.longValue() <= ioStats.maxIOTime.longValue() ? ioStats.minIOTime.longValue() : 0;
        operatorStats.setLongStat(Metric.MIN_IO_WRITE_TIME, minIOWriteTime);
        operatorStats.setLongStat(Metric.MAX_IO_WRITE_TIME, ioStats.maxIOTime.longValue());
        operatorStats.setLongStat(Metric.AVG_IO_WRITE_TIME, ioStats.numIO.get() == 0 ? 0 : ioStats.totalIOTime.longValue() / ioStats.numIO.get());
        operatorStats.addLongStat(Metric.NUM_IO_WRITE, ioStats.numIO.longValue());

        operatorStats.setProfileDetails(UserBitShared.OperatorProfileDetails
          .newBuilder()
          .addAllSlowIoInfos(ioStats.slowIOInfoList)
          .build());
      }
    } finally {
      try {
        NoExceptionAutoCloseables.close(store, pageStore, parquetFileWriter);
      } finally {
        AutoCloseables.close(new AutoCloseable() {
            @Override
            public void close() throws Exception {
              codecFactory.release();
            }
          },
          codecAllocator, columnEncoderAllocator);
      }
    }
  }

  @Override
  public FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullParquetConverter(fieldId, fieldName, reader);
  }

  public class NullParquetConverter extends ParquetFieldConverter {

    public NullParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeValue() throws IOException {
      /* NO-OP */
    }

    @Override
    public void writeField() throws IOException {
      writeValue();
    }
  }
}
