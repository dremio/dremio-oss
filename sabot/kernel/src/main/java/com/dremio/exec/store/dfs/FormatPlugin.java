/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * Similar to a storage engine but built specifically to work within a FileSystem context.
 */
public interface FormatPlugin {

  public boolean supportsRead();

  public boolean supportsWrite();

  public SabotContext getContext();

  /**
   * Indicates whether this FormatPlugin supports auto-partitioning for CTAS statements
   * @return true if auto-partitioning is supported
   */
  public boolean supportsAutoPartitioning();

  public FormatMatcher getMatcher();

  public AbstractWriter getWriter(PhysicalOperator child, String location, FileSystemPlugin plugin, WriterOptions options, OpProps props) throws IOException;

  public FormatPluginConfig getConfig();
  public String getName();

  FileDatasetHandle getDatasetAccessor(
      DatasetType type,
      PreviousDatasetInfo previousInfo,
      FileSystemWrapper fs,
      FileSelection fileSelection,
      FileSystemPlugin fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      int maxLeafColumns
  );

  /**
   * Get a record reader specifically for the purposes of previews.
   */
  public RecordReader getRecordReader(final OperatorContext context, final FileSystemWrapper dfs, final FileStatus status) throws ExecutionSetupException;
}
