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
package com.dremio.service.namespace;

import java.util.List;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * How a source exposes a table to Dremio.
 */
public interface SourceTableDefinition {
  /**
   * The fully qualified name of the dataset.
   * @return
   */
  NamespaceKey getName();

  /**
   * Get the dataset config associated with this dataset.
   * @return Configuration for this dataset.
   * @throws Exception
   */
  DatasetConfig getDataset() throws Exception;

  /**
   * Get a list of splits for this dataset. The list of splits must be 1 or greater in size.
   * @return List of splits
   * @throws Exception
   */
  List<DatasetSplit> getSplits() throws Exception;

  /**
   * Whether this table should be saved in namespace or retrieved each time requested.
   * @return True if metadata should be persisted.
   */
  boolean isSaveable();

  DatasetType getType();
}
