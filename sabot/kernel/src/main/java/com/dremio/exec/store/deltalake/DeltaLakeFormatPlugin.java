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

import java.io.IOException;

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
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;

public class DeltaLakeFormatPlugin extends BaseFormatPlugin {

  private static final String DEFAULT_NAME = "delta";

  private final SabotContext context;
  private final String name;
  private FileSystemPlugin<?> fsPlugin;
  private final DeltaLakeFormatMatcher formatMatcher;
  private final DeltaLakeFormatConfig config;
  private FormatPlugin dataFormatPlugin;

  public DeltaLakeFormatPlugin(String name, SabotContext context, DeltaLakeFormatConfig formatConfig, FileSystemPlugin<?> fsPlugin) {
      super(context, fsPlugin);
      this.context = context;
      this.config = formatConfig;
      this.name = name == null ? DEFAULT_NAME : name;
      this.fsPlugin = fsPlugin;
      this.formatMatcher = new DeltaLakeFormatMatcher(this);
      this.dataFormatPlugin = new ParquetFormatPlugin(name, context,
        new ParquetFormatConfig(), fsPlugin);
  }

  @Override
  public boolean supportsRead() {
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
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
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
  public FileDatasetHandle getDatasetAccessor(DatasetType type, PreviousDatasetInfo previousInfo, FileSystem fs, FileSelection fileSelection, FileSystemPlugin<?> fsPlugin, NamespaceKey tableSchemaPath, FileProtobuf.FileUpdateKey updateKey, int maxLeafColumns) {
    return new DeltaLakeFormatDatasetAccessor(type, fs, fileSelection, tableSchemaPath, this);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem fs, FileAttributes attributes) throws ExecutionSetupException {
    if (attributes.getPath().getName().endsWith("parquet")) {
      return dataFormatPlugin.getRecordReader(context, fs, attributes);
    } else {
      return new EmptyRecordReader();
    }
  }
}
