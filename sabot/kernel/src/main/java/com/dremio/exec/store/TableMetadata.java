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
package com.dremio.exec.store;

import java.util.Iterator;
import java.util.List;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Predicate;

/**
 * DatasetPointer interface
 */
public interface TableMetadata {
  NamespaceKey getName();

  StoragePluginId getStoragePluginId();

  /**
   * Should be moved to ReadDefinition.
   * @return
   */
  @Deprecated
  FileConfig getFormatSettings();

  /**
   * Specific configuration associated with a particular type of reader.
   * @return
   */
  ReadDefinition getReadDefinition();

  DatasetType getType();

  String computeDigest();

  String getUser();

  TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException;

  TableMetadata prune(Predicate<DatasetSplit> splitPredicate) throws NamespaceException;

  TableMetadata prune(List<DatasetSplit> newSplits) throws NamespaceException;

  /**
   * Get an opaque key to perform comparison on splits
   *
   * @return the splits key
   */
  SplitsKey getSplitsKey();

  Iterator<DatasetSplit> getSplits();

  double getSplitRatio() throws NamespaceException;

  int getSplitCount();

  BatchSchema getSchema();

  long getApproximateRecordCount();

  boolean isPruned() throws NamespaceException;
}
