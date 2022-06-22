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
package com.dremio.exec.store.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.apache.iceberg.Table;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSelectionProcessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.LayeredPluginFileSelectionProcessor;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableLoader;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Suppliers;

public class IcebergFormatPlugin extends EasyFormatPlugin<IcebergFormatConfig> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergFormatPlugin.class);

  private static final String DEFAULT_NAME = "iceberg";
  private static final boolean IS_COMPRESSIBLE = true;

  private final SabotContext context;
  private final IcebergFormatMatcher formatMatcher;
  private final IcebergFormatConfig config;
  private final String name;
  private SupportsIcebergRootPointer fsPlugin;
  private FormatPlugin dataFormatPlugin;
  private final boolean isLayered;

  public IcebergFormatPlugin(String name, SabotContext context, IcebergFormatConfig formatConfig, FileSystemPlugin<?> fsPlugin) {
    super(name, context, formatConfig, true, false, false, IS_COMPRESSIBLE, formatConfig.getExtensions(), DEFAULT_NAME, fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.name = name == null ? DEFAULT_NAME : name;
    this.formatMatcher = new IcebergFormatMatcher(this);
    this.isLayered = true;
    if (fsPlugin != null) {
      initialize(formatConfig, fsPlugin);
    }
  }

  public void initialize(IcebergFormatConfig formatConfig, SupportsIcebergRootPointer fsPlugin) {
    this.fsPlugin = fsPlugin;
    if (formatConfig.getDataFormatType() == FileType.PARQUET && fsPlugin instanceof FileSystemPlugin) {
      dataFormatPlugin = new ParquetFormatPlugin(name, context,
              (ParquetFormatConfig) formatConfig.getDataFormatConfig(), (FileSystemPlugin<?>) fsPlugin);
    }
  }

  @Override
  public IcebergFormatConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean isLayered() { return this.isLayered; }

  // TODO ravindra: should get the parquet file path by traversing the json file.
  @Override
  public RecordReader getRecordReader(
      OperatorContext context, FileSystem fs, FileAttributes attributes)
      throws ExecutionSetupException {
    if (attributes.getPath().getName().endsWith("parquet") && dataFormatPlugin != null) {
      return dataFormatPlugin.getRecordReader(context, fs, attributes);
    } else {
      return new EmptyRecordReader();
    }
  }

  @Override
  public FileSelectionProcessor getFileSelectionProcessor(FileSystem fs, FileSelection fileSelection) {
    return new LayeredPluginFileSelectionProcessor(fs, fileSelection);
  }

  @Override
  public String getName(){
    return name;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return true;
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
  public RecordReader getRecordReader(OperatorContext context,
                                      FileSystem dfs,
                                      EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                      List<SchemaPath> columns) throws ExecutionSetupException {
    throw new UnsupportedOperationException("unimplemented method");
  }

  @Override
  public RecordReader getRecordReader(
    OperatorContext context,
    FileSystem dfs,
    SplitAndPartitionInfo split,
    EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
    List<SchemaPath> columns,
    FragmentExecutionContext fec,
    EasySubScan config) throws ExecutionSetupException {
    throw new UnsupportedOperationException("Deprecated path");
  }

  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.ICEBERG_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return UserBitShared.CoreOperatorType.MANIFEST_WRITER_VALUE;
  }

  @Override
  public FileDatasetHandle getDatasetAccessor(
      DatasetType type,
      PreviousDatasetInfo previousInfo,
      FileSystem fs,
      FileSelection fileSelection,
      FileSystemPlugin<?> fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      int maxLeafColumns,
      TimeTravelOption.TimeTravelRequest travelRequest
  ) {
    if (!context.getOptionManager().getOption(ExecConstants.ENABLE_ICEBERG)) {
      throw new UnsupportedOperationException("Please contact customer support for steps to enable " +
          "the iceberg tables feature.");
    }

    final Supplier<Table> tableSupplier = Suppliers.memoize(
        () -> {
          final IcebergModel icebergModel = fsPlugin.getIcebergModel();
          final IcebergTableLoader icebergTableLoader = icebergModel.getIcebergTableLoader(
              icebergModel.getTableIdentifier(fileSelection.getSelectionRoot()));
          return icebergTableLoader.getIcebergTable();
        }
    );

    final TableSnapshotProvider tableSnapshotProvider =
        TimeTravelProcessors.getTableSnapshotProvider(tableSchemaPath.getPathComponents(), travelRequest);
    final TableSchemaProvider tableSchemaProvider =
        TimeTravelProcessors.getTableSchemaProvider(travelRequest);
    return new IcebergExecutionDatasetAccessor(MetadataObjectsUtils.toEntityPath(tableSchemaPath),
        tableSupplier, fsPlugin.getFsConfCopy(), this, fs, tableSnapshotProvider, fsPlugin, tableSchemaProvider);
  }

  @Override
  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException("Deprecated path");
  }
}
