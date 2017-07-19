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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.util.Set;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.FileUpdateKey;

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

  public AbstractWriter getWriter(PhysicalOperator child, String userName, String location, FileSystemPlugin plugin, WriterOptions options) throws IOException;

  public Set<StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerRulesContext);

  public FormatPluginConfig getConfig();
  public StoragePluginConfig getStorageConfig();
  public String getName();
  public FileSystemDatasetAccessor getDatasetAccessor(DatasetConfig oldConfig, FileSystemWrapper fs, FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, String tableName, FileUpdateKey updateKey);
}
