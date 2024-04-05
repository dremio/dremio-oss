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
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_REMOVE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.VERSION;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.hadoop.HadoopCompressionCodecFactory;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileCountTooLargeException;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSelectionProcessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.LayeredPluginFileSelectionProcessor;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyScanOperatorCreator;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.easy.json.JSONRecordReader;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.parquet.BulkInputStream;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.Streams;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaLakeFormatPlugin extends EasyFormatPlugin<DeltaLakeFormatConfig> {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeFormatPlugin.class);
  private static final String DEFAULT_NAME = "delta";
  private static final boolean IS_COMPRESSIBLE = true;

  private static final String NOT_SUPPORT_METASTORE_TABLE_MSG =
      "This folder does not contain a filesystem-based Delta Lake table.";

  private final SabotContext context;
  private final String name;
  private final DeltaLakeFormatMatcher formatMatcher;
  private final DeltaLakeFormatConfig config;
  private FormatPlugin dataFormatPlugin;

  public DeltaLakeFormatPlugin(
      String name,
      SabotContext context,
      DeltaLakeFormatConfig formatConfig,
      FileSystemPlugin<?> fsPlugin) {
    super(
        name,
        context,
        formatConfig,
        true,
        false,
        false,
        IS_COMPRESSIBLE,
        formatConfig.getExtensions(),
        DEFAULT_NAME,
        fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.name = name == null ? DEFAULT_NAME : name;
    this.formatMatcher = new DeltaLakeFormatMatcher(this);
    this.dataFormatPlugin =
        new ParquetFormatPlugin(name, context, new ParquetFormatConfig(), fsPlugin);
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
  public AbstractWriter getWriter(
      PhysicalOperator child,
      String location,
      FileSystemPlugin<?> plugin,
      WriterOptions options,
      OpProps props)
      throws IOException {
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
  public FileDatasetHandle getDatasetAccessor(
      DatasetType type,
      PreviousDatasetInfo previousInfo,
      FileSystem fs,
      FileSelection fileSelection,
      FileSystemPlugin fsPlugin,
      NamespaceKey tableSchemaPath,
      FileProtobuf.FileUpdateKey updateKey,
      int maxLeafColumns,
      TimeTravelOption.TimeTravelRequest timeTravelRequest) {
    return new DeltaLakeFormatDatasetAccessor(
        type, fs, fsPlugin, fileSelection, tableSchemaPath, this, timeTravelRequest);
  }

  @Override
  public RecordReader getRecordReader(
      OperatorContext context, FileSystem fs, FileAttributes attributes)
      throws ExecutionSetupException {
    if (isParquet(fs, attributes)) {
      return dataFormatPlugin.getRecordReader(context, fs, attributes);
    } else {
      return new EmptyRecordReader();
    }
  }

  @Override
  public FileSelectionProcessor getFileSelectionProcessor(
      FileSystem fs, FileSelection fileSelection) {
    return new LayeredPluginFileSelectionProcessor(fs, fileSelection);
  }

  @VisibleForTesting
  static boolean isParquet(FileSystem fs, FileAttributes attributes) {
    long fileLen = attributes.size();
    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen
        < MAGIC.length
            + FOOTER_LENGTH_SIZE
            + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      logger.info("File {} is too small to be a parquet", attributes.getPath());
      return false;
    }

    if (attributes.getPath().getName().endsWith("parquet")) {
      return true; // Avoid unnecessary IO. Trust this to be a parquet.
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

  @Override
  public RecordReader getRecordReader(
      OperatorContext context,
      FileSystem dfs,
      EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns)
      throws ExecutionSetupException {
    // This plugin should always use overridden getRecordReader() method
    throw new NotImplementedException("Unexpected invocation");
  }

  private boolean readingAddLogsOfPartitionedDataset(
      EasySubScan scanConfig, List<SchemaPath> columns) {
    return CollectionUtils.isNotEmpty(scanConfig.getPartitionColumns())
        && // partitioned table
        !columns.contains(
            SchemaPath.getCompoundPath(
                DELTA_FIELD_REMOVE, SCHEMA_PATH)); /* not scanning for 'remove' logs */
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean isLayered() {
    return true;
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

  @Override
  protected RecordReaderIterator getRecordReaderIterator(
      FileSystem fs,
      OperatorContext opCtx,
      List<SchemaPath> innerFields,
      EasySubScan easyScanConfig,
      List<EasyScanOperatorCreator.SplitAndExtended> workList) {
    final DeltaCheckpointParquetSplitReaderCreator parquetSplitReaderCreator =
        new DeltaCheckpointParquetSplitReaderCreator(fs, opCtx, easyScanConfig);

    final Stream<RecordReader> readers =
        workList.stream()
            .map(
                input -> {
                  try {
                    final EasyProtobuf.EasyDatasetSplitXAttr easyXAttr = input.getExtended();
                    final Path inputFilePath = Path.of(easyXAttr.getPath());

                    DeltaLakeProtobuf.DeltaCommitLogSplitXAttr deltaExtended;
                    deltaExtended =
                        DeltaLakeProtobuf.DeltaCommitLogSplitXAttr.parseFrom(
                            easyXAttr.getExtendedProperty());

                    final long version = deltaExtended.getVersion();
                    RecordReader deltaRecordReader;

                    if (!fs.supportsPath(inputFilePath)) {
                      throw UserException.invalidMetadataError()
                          .addContext(
                              String.format(
                                  "%s: Invalid FS for file '%s'",
                                  fs.getScheme(), input.getExtended().getPath()))
                          .addContext("File", input.getExtended().getPath())
                          .setAdditionalExceptionContext(
                              new InvalidMetadataErrorContext(
                                  ImmutableList.copyOf(easyScanConfig.getReferencedTables())))
                          .build(logger);
                    }

                    final boolean addWithPartitionCols =
                        readingAddLogsOfPartitionedDataset(easyScanConfig, innerFields);
                    if (easyXAttr.getPath().endsWith("json")) {
                      deltaRecordReader =
                          getCommitJsonRecordReader(
                              fs,
                              opCtx,
                              easyScanConfig,
                              addWithPartitionCols,
                              easyXAttr,
                              innerFields);
                    } else {
                      deltaRecordReader =
                          parquetSplitReaderCreator.getParquetRecordReader(
                              fs, input, addWithPartitionCols);
                    }
                    // Wrap the record reader to have the version column as additional columns
                    return new AdditionalColumnsRecordReader(
                        opCtx,
                        deltaRecordReader,
                        Arrays.asList(
                            new ConstantColumnPopulators.BigIntNameValuePair(VERSION, version)),
                        context.getAllocator());
                  } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw UserException.dataReadError(e)
                        .addContext(
                            "Unable to retrive version info of commit ",
                            input.getExtended().getPath())
                        .build(logger);
                  } catch (ExecutionSetupException e) {
                    if (e.getCause() instanceof FileNotFoundException) {
                      throw UserException.invalidMetadataError(e.getCause())
                          .addContext("File not found")
                          .addContext("File", input.getExtended().getPath())
                          .setAdditionalExceptionContext(
                              new InvalidMetadataErrorContext(
                                  ImmutableList.copyOf(easyScanConfig.getReferencedTables())))
                          .build(logger);
                    } else {
                      throw new RuntimeException(e);
                    }
                  }
                });
    return RecordReaderIterator.from(readers.iterator());
  }

  private RecordReader getCommitJsonRecordReader(
      FileSystem fs,
      OperatorContext opCtx,
      EasySubScan easyScanConfig,
      boolean addWithPartitionCols,
      EasyProtobuf.EasyDatasetSplitXAttr easyXAttr,
      List<SchemaPath> innerFields) {
    String path = easyXAttr.getPath();
    if (fs != null && !fs.supportsPathsWithScheme()) {
      path = Path.getContainerSpecificRelativePath(Path.of(path));
    }
    final JSONRecordReader jsonRecordReader =
        new JSONRecordReader(opCtx, path, getCodecFactory(), fs, innerFields);
    jsonRecordReader.resetSpecialSchemaOptions();

    if (addWithPartitionCols) {
      // Fetch list of partition columns from add.partitionValues_parsed
      final List<Field> partitionCols =
          easyScanConfig.getFullSchema().findField(DELTA_FIELD_ADD).getChildren().stream()
              .filter(field -> field.getName().equals(SCHEMA_PARTITION_VALUES_PARSED))
              .flatMap(field -> field.getChildren().stream())
              .collect(Collectors.toList());

      // Replace actual partition col fields instead of projecting all columns. When a table is
      // repartitioned on different
      // column, we don't want old commit log json to promote incorrect partition cols.
      final SchemaPath partitionValuesPath =
          SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES);
      final SchemaPath partitionValuesParsedPath =
          SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES_PARSED);
      final List<SchemaPath> projectedCols = new ArrayList<>(innerFields);
      projectedCols.removeIf(
          p -> p.equals(partitionValuesPath) || p.equals(partitionValuesParsedPath));
      partitionCols.forEach(p -> projectedCols.add(partitionValuesPath.getChild(p.getName())));

      return new DeltaLogCommitJsonRecordReader(opCtx, jsonRecordReader, partitionCols);
    } else {
      return jsonRecordReader;
    }
  }

  private CompressionCodecFactory getCodecFactory() {
    return getFsPlugin() != null
        ? getFsPlugin().getCompressionCodecFactory()
        : HadoopCompressionCodecFactory.DEFAULT;
  }

  @Override
  public DirectoryStream<FileAttributes> getFilesForSamples(
      FileSystem fs, FileSystemPlugin<?> fsPlugin, Path path)
      throws IOException, FileCountTooLargeException {
    if (!formatMatcher.isDeltaLakeTable(fs, path.toString())) {
      throw UserException.unsupportedError()
          .message(NOT_SUPPORT_METASTORE_TABLE_MSG)
          .buildSilently();
    }
    return super.getFilesForSamples(fs, fsPlugin, path);
  }
}
