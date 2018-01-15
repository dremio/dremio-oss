/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVectorHelper;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStoreExposer;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.ParquetOutputRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ParquetRecordWriter extends ParquetOutputRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordWriter.class);

  public enum Metric implements MetricDef {
    NUM_FILES_WRITTEN, // number of files written by the writer
    MIN_FILE_SIZE, // Minimum size of files written
    MAX_FILE_SIZE, // Maximum size of files written
    AVG_FILE_SIZE, // Average size of files written
    MIN_RECORD_COUNT_IN_FILE, // Minimum number of records written in a file
    MAX_RECORD_COUNT_IN_FILE, // Maximum number of records written in a file
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public static final String DREMIO_ARROW_SCHEMA = "dremio.arrow.schema";

  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  public static final String DRILL_VERSION_PROPERTY = "drill.version";
  public static final String DREMIO_VERSION_PROPERTY = "dremio.version";
  public static final String IS_DATE_CORRECT_PROPERTY = "is.date.correct";
  public static final String WRITER_VERSION_PROPERTY = "drill-writer.version";

  private final Configuration conf;
  private final UserGroupInformation proxyUserUGI;

  private final BufferAllocator codecAllocator;
  private final BufferAllocator columnEncoderAllocator;

  private ParquetFileWriter parquetFileWriter;
  private MessageType schema;
  private Map<String, String> extraMetaData = new HashMap<>();
  private int blockSize;
  private int pageSize;
  private boolean enableDictionary = false;
  private boolean enableDictionaryForBinary = false;
  private CompressionCodecName codec = CompressionCodecName.SNAPPY;
  private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;
  private CodecFactory codecFactory;
  private FileSystemWrapper fs;
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

  // metrics workspace variables
  int numFilesWritten = 0;
  long minFileSize = Long.MAX_VALUE;
  long maxFileSize = Long.MIN_VALUE;
  long avgFileSize = 0;
  long minRecordCountInFile = Long.MAX_VALUE;
  long maxRecordCountInFile = Long.MIN_VALUE;

  public ParquetRecordWriter(OperatorContext context, ParquetWriter writer, ParquetFormatConfig config) throws OutOfMemoryException{
    this.conf = new Configuration(writer.getFsConf());
    this.context = context;
    this.codecAllocator = context.getAllocator().newChildAllocator("ParquetCodecFactory", 0, Long.MAX_VALUE);
    this.columnEncoderAllocator = context.getAllocator().newChildAllocator("ParquetColEncoder", 0, Long.MAX_VALUE);
    this.codecFactory = CodecFactory.createDirectCodecFactory(this.conf,
        new ParquetDirectByteBufferAllocator(codecAllocator), pageSize);
    this.extraMetaData.put(DREMIO_VERSION_PROPERTY, DremioVersionInfo.getVersion());
    this.extraMetaData.put(IS_DATE_CORRECT_PROPERTY, "true");
    this.proxyUserUGI = ImpersonationUtil.createProxyUgi(writer.getUserName());

    FragmentHandle handle = context.getFragmentHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());

    this.location = writer.getLocation();
    this.prefix = fragmentId;
    this.extension = config.outputExtension;

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
  }

  @Override
  public void setup() throws IOException {
    this.fs = FileSystemWrapper.get(conf);
    this.batchSchema = incoming.getSchema();
    newSchema();

  }


  /**
   * Helper method to create a new {@link ParquetFileWriter} as impersonated user.
   * @throws IOException
   */
  private void initRecordReader() throws IOException {

    this.path = fs.canonicalizePath(partition.qualified(location, prefix + "_" + index + "." + extension));
    try {
      parquetFileWriter =
          proxyUserUGI.doAs(new PrivilegedExceptionAction<ParquetFileWriter>() {
            @Override
            public ParquetFileWriter run() throws Exception {
              final ParquetFileWriter parquetFileWriter =
                  new ParquetFileWriter(checkNotNull(conf), checkNotNull(schema), path, ParquetFileWriter.Mode.CREATE, DEFAULT_BLOCK_SIZE,
                    MAX_PADDING_SIZE_DEFAULT, true);
              parquetFileWriter.start();
              return parquetFileWriter;
            }
          });
    } catch (InterruptedException e) {
      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while creating the parquet record writer", e);
    }
  }

  private void newSchema() throws IOException {
    // Reset it to half of current number and bound it within the limits
    recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCountForNextMemCheck / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);

    String json = new Schema(batchSchema).toJson();
    extraMetaData.put(DREMIO_ARROW_SCHEMA, json);
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
    schema = new MessageType("root", types);

    int dictionarySize = (int)context.getOptions().getOption(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR);
    final ParquetProperties parquetProperties = new ParquetProperties(dictionarySize, writerVersion, enableDictionary,
      new ParquetDirectByteBufferAllocator(columnEncoderAllocator), pageSize, true, enableDictionaryForBinary);
    pageStore = ColumnChunkPageWriteStoreExposer.newColumnChunkPageWriteStore(codecFactory.getCompressor(codec), schema, parquetProperties);
    store = new ColumnWriteStoreV1(pageStore, pageSize, parquetProperties);
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

  @Nullable
  private Type getType(Field field) {
    MinorType minorType = getMajorTypeForField(field).getMinorType();
    switch(minorType) {
      case MAP: {
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
        null);
    } else {
      GroupType childGroupType = childType.asGroupType();
      return new GroupType(childType.getRepetition(),
        "element",
        childType.getOriginalType(),
        childGroupType.getFields());
    }
  }


  private void flushAndClose() throws IOException {
    if(parquetFileWriter == null){
      return;
    }

    if (recordCount > 0) {
      long memSize = store.getBufferedSize();
      parquetFileWriter.startBlock(recordCount);
      consumer.flush();
      store.flush();
      ColumnChunkPageWriteStoreExposer.flushPageStore(pageStore, parquetFileWriter);
      parquetFileWriter.endBlock();
      long recordsWritten = recordCount;

      // we are writing one single block per file
      parquetFileWriter.end(extraMetaData);
      byte[] metadata = this.trackingConverter == null ? null : trackingConverter.getMetadata();
      listener.recordsWritten(recordsWritten, path.toString(), metadata /** TODO: add parquet footer **/, partition.getBucketNumber());
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

  private interface UpdateTrackingConverter {
    public byte[] getMetadata();
  }

  private static class UpdateBigIntTrackingConverter extends FieldConverter implements UpdateTrackingConverter {

    private Long max;

    public UpdateBigIntTrackingConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      if(max == null){
        max = reader.readLong();
      }else {
        max = Math.max(max,  reader.readLong());
      }
    }

    @Override
    public byte[] getMetadata() {
      if(max != null){
        // TODO replace with better serialization
        return Long.toString(max).getBytes();
      }
      return null;
    }
  }

  private static class UpdateTimestampTrackingConverter extends FieldConverter implements UpdateTrackingConverter {

    private Long max;
    private final NullableTimeStampMilliHolder holder = new NullableTimeStampMilliHolder();

    public UpdateTimestampTrackingConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      if(max == null){
        reader.read(holder);
        max = holder.value;
      }else {
        max = Math.max(max,  reader.readLong());
      }
    }

    @Override
    public byte[] getMetadata() {
      if(max != null){
        // TODO replace with better serialization
        return Long.toString(max).getBytes();
      }
      return null;
    }

  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    if(IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)){
      UpdateBigIntTrackingConverter c = new UpdateBigIntTrackingConverter(fieldId, fieldName, reader);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableBigIntConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTimeStampMilliConverter(int fieldId, String fieldName, FieldReader reader) {
    if(IncrementalUpdateUtils.UPDATE_COLUMN.equals(fieldName)){
      UpdateTimestampTrackingConverter c = new UpdateTimestampTrackingConverter(fieldId, fieldName, reader);
      Preconditions.checkArgument(this.trackingConverter == null, "More than one update field found.");
      this.trackingConverter = c;
      return c;
    }
    return super.getNewNullableTimeStampMilliConverter(fieldId, fieldName, reader);
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
    UnionReader unionReader = (UnionReader)reader;
    MapVector internalMap = new UnionVectorHelper(unionReader.data).getInternalMap();
    SingleMapReaderImpl mapReader = new SingleMapReaderImpl(internalMap);
    return getNewMapConverter(fieldId, fieldName, mapReader);
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
      initRecordReader();
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
    AutoCloseables.close(new AutoCloseable() {
      @Override
      public void close() throws Exception {
        flushAndClose();
      }
    }, new AutoCloseable() {
      @Override
      public void close() throws Exception {
        codecFactory.release();
      }
    },
    codecAllocator, columnEncoderAllocator);
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
