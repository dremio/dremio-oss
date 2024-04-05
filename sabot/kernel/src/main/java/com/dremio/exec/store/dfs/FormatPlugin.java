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
package com.dremio.exec.store.dfs;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import java.io.IOException;
import java.nio.file.DirectoryStream;

/** Similar to a storage engine but built specifically to work within a FileSystem context. */
public interface FormatPlugin {
  public boolean supportsRead();

  public boolean supportsWrite();

  // Is a layer on top of other single/multiple file formats.
  default boolean isLayered() {
    return false;
  }

  public SabotContext getContext();

  /**
   * Indicates whether this FormatPlugin supports auto-partitioning for CTAS statements
   *
   * @return true if auto-partitioning is supported
   */
  public boolean supportsAutoPartitioning();

  public FormatMatcher getMatcher();

  public AbstractWriter getWriter(
      PhysicalOperator child,
      String location,
      FileSystemPlugin<?> plugin,
      WriterOptions options,
      OpProps props)
      throws IOException;

  public FormatPluginConfig getConfig();

  public String getName();

  FileDatasetHandle getDatasetAccessor(
      DatasetType type,
      PreviousDatasetInfo previousInfo,
      FileSystem fs,
      FileSelection fileSelection,
      FileSystemPlugin<?> fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      int maxLeafColumns,
      TimeTravelOption.TimeTravelRequest timeTravelRequest);

  /** Get a record reader specifically for the purposes of previews. */
  public RecordReader getRecordReader(
      final OperatorContext context, final FileSystem dfs, final FileAttributes attributes)
      throws ExecutionSetupException;

  default FileSelectionProcessor getFileSelectionProcessor(
      FileSystem fs, FileSelection fileSelection) {
    return new DefaultFileSelectionProcessor(fs, fileSelection, getMaxFilesLimit());
  }

  /** Get the files under a path for sample data purpose */
  DirectoryStream<FileAttributes> getFilesForSamples(
      FileSystem fs, FileSystemPlugin<?> fsPlugin, Path path)
      throws IOException, FileCountTooLargeException;

  /**
   * @return Returns the max number of files supported by this format plugin
   */
  default int getMaxFilesLimit() {
    return Math.toIntExact(
        getContext().getOptionManager().getOption(FileDatasetHandle.DFS_MAX_FILES));
  }
}
