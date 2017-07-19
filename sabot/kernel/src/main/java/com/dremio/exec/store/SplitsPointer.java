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

import com.dremio.datastore.SearchTypes;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

/**
 * Interface for split pointer
 */
public interface SplitsPointer {

  /**
   * get splits ratio
   * @return
   * @throws NamespaceException
   */
  double getSplitRatio() throws NamespaceException;

  /**
   * get total number of splits for the dataset
   * @return
   * @throws NamespaceException
   */
  int getSplitsCount() throws NamespaceException;

  /**
   * Apply filter query and prune partitions.
   * @param partitionFilterQuery
   * @return
   * @throws NamespaceException
   */
  SplitsPointer prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException;

  /**
   * Iterable for splits.
   * @return
   */
  Iterable<DatasetSplit> getSplitIterable();


  /**
   * Materialize this splits in this split pointer if they aren't already
   * materialized. Do this when you might expect to retrieve the splits multiple
   * times.
   */
  void materialize();

  /**
   * id
   * @return
   */
  String computeDigest();

  int getTotalSplitsCount();

  boolean isPruned() throws NamespaceException;
}
