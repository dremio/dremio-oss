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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.BaseFormatPlugin;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileType;

public class IcebergFormatPlugin extends BaseFormatPlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergFormatPlugin.class);

  private static final String DEFAULT_NAME = "iceberg";

  private final SabotContext context;
  private final IcebergFormatMatcher formatMatcher;
  private final IcebergFormatConfig config;
  private final String name;
  private FileSystemPlugin<?> fsPlugin;
  private FormatPlugin dataFormatPlugin;

  public IcebergFormatPlugin(String name, SabotContext context, IcebergFormatConfig formatConfig, FileSystemPlugin<?> fsPlugin) {
    super(context, fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.name = name == null ? DEFAULT_NAME : name;
    this.fsPlugin = fsPlugin;
    this.formatMatcher = new IcebergFormatMatcher(this);

    if (formatConfig.getDataFormatType() == FileType.PARQUET) {
      dataFormatPlugin = new ParquetFormatPlugin(name, context,
        (ParquetFormatConfig)formatConfig.getDataFormatConfig(), fsPlugin);
    } else {
      throw new UnsupportedOperationException("iceberg does not support data format type " + formatConfig.getDataFormatType());
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
  public boolean isLayered() { return true; }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, FileSystemPlugin<?> plugin, WriterOptions options, OpProps props) {
    throw new UnsupportedOperationException("iceberg writer not implemented");
  }

  // TODO ravindra: should get the parquet file path by traversing the json file.
  @Override
  public RecordReader getRecordReader(
      OperatorContext context, FileSystem fs, FileAttributes attributes)
      throws ExecutionSetupException {
    if (attributes.getPath().getName().endsWith("parquet")) {
      return dataFormatPlugin.getRecordReader(context, fs, attributes);
    } else {
      return new EmptyRecordReader();
    }
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
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }

  @Override
  public FileDatasetHandle getDatasetAccessor(DatasetType type, PreviousDatasetInfo previousInfo, FileSystem fs,
      FileSelection fileSelection, FileSystemPlugin<?> fsPlugin, NamespaceKey tableSchemaPath, FileUpdateKey updateKey,
      int maxLeafColumns) {
    return new IcebergFormatDatasetAccessor(type, fs, fileSelection, tableSchemaPath,
      this, fsPlugin, previousInfo, maxLeafColumns);
  }
}
