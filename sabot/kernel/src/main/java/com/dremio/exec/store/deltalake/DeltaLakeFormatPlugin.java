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
package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY_VALUE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_VALUE;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.easy.json.JSONRecordReader;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.parquet.BulkInputStream;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.Streams;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

public class DeltaLakeFormatPlugin extends EasyFormatPlugin<DeltaLakeFormatConfig> {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeFormatPlugin.class);
  private static final String DEFAULT_NAME = "delta";
  private static final boolean IS_COMPRESSIBLE = true;

  private final SabotContext context;
  private final String name;
  private final DeltaLakeFormatMatcher formatMatcher;
  private final DeltaLakeFormatConfig config;
  private FormatPlugin dataFormatPlugin;
  private final boolean isLayered;

  public DeltaLakeFormatPlugin(String name, SabotContext context, DeltaLakeFormatConfig formatConfig, FileSystemPlugin<?> fsPlugin) {
    super(name, context, formatConfig, true, false, false, IS_COMPRESSIBLE, formatConfig.getExtensions(), DEFAULT_NAME, fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.name = name==null ? DEFAULT_NAME:name;
    this.formatMatcher = new DeltaLakeFormatMatcher(this);
    this.dataFormatPlugin = new ParquetFormatPlugin(name, context,
            new ParquetFormatConfig(), fsPlugin);
    boolean useDeltaExecution = context.getOptionManager().getOption(PlannerSettings.ENABLE_DELTALAKE);
    isLayered = !useDeltaExecution;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.DELTALAKE_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    // this.supportsWrite() returns false. Hence, this method isn't expected to be invoked.
    throw new UnsupportedOperationException("Writing DeltaLake tables aren't supported.");
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, FileSystemPlugin<?> plugin, WriterOptions options, OpProps props) throws IOException {
    return null;
  }

  @Override
  public DeltaLakeFormatConfig getConfig() {
    return config;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public FileDatasetHandle getDatasetAccessor(DatasetType type,
                                              PreviousDatasetInfo previousInfo,
                                              FileSystem fs,
                                              FileSelection fileSelection,
                                              FileSystemPlugin fsPlugin,
                                              NamespaceKey tableSchemaPath,
                                              FileProtobuf.FileUpdateKey updateKey,
                                              int maxLeafColumns) {
    return new DeltaLakeFormatDatasetAccessor(type, fs, fsPlugin, fileSelection, tableSchemaPath, this);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem fs, FileAttributes attributes) throws ExecutionSetupException {
    if (isParquet(fs, attributes)) {
      return dataFormatPlugin.getRecordReader(context, fs, attributes);
    } else {
      return new EmptyRecordReader();
    }
  }

  @VisibleForTesting
  static boolean isParquet(FileSystem fs, FileAttributes attributes) {
    long fileLen = attributes.size();
    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      logger.info("File {} is too small to be a parquet", attributes.getPath());
      return false;
    }

    if (attributes.getPath().getName().endsWith("parquet")) {
      return true;//Avoid unnecessary IO. Trust this to be a parquet.
    }

    // Check if the file has parquet magic bytes before concluding that it's not a parquet
    try (BulkInputStream is = BulkInputStream.wrap(Streams.wrap(fs.open(attributes.getPath())))) {
      logger.debug("Checking file {} if it contains parquet magic bytes", attributes.getPath());
      byte[] magic = new byte[MAGIC.length];
      is.readFully(magic, 0, MAGIC.length);
      return Arrays.equals(MAGIC, magic);
    } catch (Exception e) {
      logger.warn("Error while reading " + attributes.getPath(), e);
      return false;
    }
  }

  public RecordReader getRecordReader(
          OperatorContext context,
          FileSystem dfs,
          EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
          List<SchemaPath> columns) throws ExecutionSetupException {
    // This plugin should always use overridden getRecordReader() method
    throw new NotImplementedException("Unexpected invocation");
  }

  private boolean readingAddLogsOfPartitionedDataset(EasySubScan scanConfig, List<SchemaPath> columns) {
    return CollectionUtils.isNotEmpty(scanConfig.getPartitionColumns()) && // partitioned table
      columns.contains(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PATH)); /* scanning for 'add' logs */
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context,
                                      FileSystem dfs,
                                      SplitAndPartitionInfo split,
                                      EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                      List<SchemaPath> columns,
                                      FragmentExecutionContext fec,
                                      EasySubScan subScanConfig) throws ExecutionSetupException {
    if (splitAttributes.getPath().endsWith("json")) {
      JSONRecordReader jsonRecordReader = new JSONRecordReader(context, splitAttributes.getPath(), getFsPlugin().getCompressionCodecFactory(), dfs, columns);

      if (readingAddLogsOfPartitionedDataset(subScanConfig, columns)) {
        return new DeltaLogCommitJsonRecordReader(context, jsonRecordReader);
      } else {
        return jsonRecordReader;
      }

    } else if (splitAttributes.getPath().endsWith("parquet")) {
      return getParquetRecordReader(context, split, splitAttributes, fec, subScanConfig, columns);
    } else {
      throw new ExecutionSetupException("Invalid split file type. Expected json | parquet. Path - " + splitAttributes.getPath());
    }
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean isLayered() {
    return this.isLayered;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public SabotContext getContext() {
    return context;
  }

  @Override
  public boolean supportsPushDown() {
    return false;
  }

  private RecordReader getParquetRecordReader(OperatorContext context,
                                              SplitAndPartitionInfo split,
                                              EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                              FragmentExecutionContext fec,
                                              EasySubScan subScanConfig,
                                              List<SchemaPath> columns) throws ExecutionSetupException {
    // TODO: Set and use row group based parquet splits.
    final boolean isArrowCachingEnabled = context.getOptions().getOption(ExecConstants.ENABLE_BOOSTING);
    boolean addWithPartitionCols = readingAddLogsOfPartitionedDataset(subScanConfig, columns);
    final ParquetSubScan parquetSubScan = toParquetScanConfig(subScanConfig, isArrowCachingEnabled, addWithPartitionCols);

    final ParquetSplitReaderCreatorIterator splitIt = new ParquetSplitReaderCreatorIterator(fec, context, parquetSubScan, false);
    splitIt.setIgnoreSchemaLearning(true);
    final SplitAndPartitionInfo parquetSplit = toParquetSplit(split, splitAttributes);
    final RecordReaderIterator recordReaderIterator = splitIt.getReaders(ImmutableList.of(parquetSplit));
    Preconditions.checkState(recordReaderIterator.hasNext(), "Error while initialising parquet RecordReader for " + splitAttributes.getPath());
    RecordReader parquetReader = recordReaderIterator.next();

    if (addWithPartitionCols) {
      final List<Field> partitionCols = subScanConfig.getPartitionColumns().stream()
        .map(c -> subScanConfig.getFullSchema().findField(c)).collect(Collectors.toList());
      return new DeltaLogCheckpointParquetRecordReader(context, parquetReader, partitionCols, parquetSubScan);
    } else {
      return parquetReader;
    }
  }

  private SplitAndPartitionInfo toParquetSplit(final SplitAndPartitionInfo split, EasyProtobuf.EasyDatasetSplitXAttr easyXAttr) {
    final ParquetProtobuf.ParquetBlockBasedSplitXAttr.Builder parquetXAttr = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
            .setPath(easyXAttr.getPath())
            .setLength(easyXAttr.getLength())
            .setStart(easyXAttr.getStart())
            .setFileLength(easyXAttr.getUpdateKey().getLength())
            .setLastModificationTime(easyXAttr.getUpdateKey().getLastModificationTime());
    final PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
            .newBuilder(split.getDatasetSplitInfo())
            .setExtendedProperty(parquetXAttr.build().toByteString())
            .build();

    return new SplitAndPartitionInfo(split.getPartitionInfo(), splitInfo);
  }

  private ParquetSubScan toParquetScanConfig(final EasySubScan easyConfig,
                                             final boolean arrowCachingEnabled, boolean addWithPartitionCols) {
    // Checkpoint parquet should be scanned as a regular PARQUET instead of DELTA type
    final FileConfig formatSettings = ProtostuffUtil.copy(easyConfig.getFileConfig());
    formatSettings.setType(FileType.PARQUET);
    List<SchemaPath> columns = new ArrayList<>(easyConfig.getColumns());
    BatchSchema fullSchema = easyConfig.getFullSchema().clone();
    if (addWithPartitionCols) {
      Field addField = fullSchema.findField(DELTA_FIELD_ADD);
      Field addFieldCopy = new Field(addField.getName(), addField.getFieldType(), addField.getChildren());
      List<Field> children = addFieldCopy.getChildren();
      children.removeIf(f -> f.getName().equals(SCHEMA_PARTITION_VALUES)); // struct

      final Field partitionKey = Field.nullablePrimitive(SCHEMA_KEY, new ArrowType.Utf8());
      final Field partitionVal = Field.nullablePrimitive(SCHEMA_VALUE, new ArrowType.Utf8());
      final Field partitionEntry = new Field("$data$", FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(partitionKey, partitionVal));
      final Field partitionKeyVal = new Field(SCHEMA_KEY_VALUE, FieldType.nullable(new ArrowType.List()), ImmutableList.of(partitionEntry));
      final Field partitionValues = new Field(SCHEMA_PARTITION_VALUES, FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(partitionKeyVal)); // Map type is currently not supported
      children.add(partitionValues); // map

      List<Field> newFields = new ArrayList<>(fullSchema.getFields());
      newFields.removeIf(f -> f.getName().equals(DELTA_FIELD_ADD));
      newFields.add(addFieldCopy);

      fullSchema = new BatchSchema(newFields);

    } else {
      // remove partition fields in projected columns
      columns.removeIf(s -> s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES)) ||
        s.equals(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES_PARSED)));
    }

    final ParquetSubScan parquetConfig = new ParquetSubScan(
            easyConfig.getProps(),
            formatSettings,
            Collections.emptyList(), // initialise with no splits to avoid redundant footer read.
            fullSchema,
            ImmutableList.of(easyConfig.getTableSchemaPath()),
            Collections.emptyList(),
            easyConfig.getPluginId(),
            columns,
            easyConfig.getPartitionColumns(),
            Collections.emptyList(),
            easyConfig.getExtendedProperty(),
            arrowCachingEnabled);
    return parquetConfig;
  }
}
