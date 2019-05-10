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
package com.dremio.connector.metadata;

/**
 * This is a marker interface that represents a handle to a dataset.
 * <p>
 * Implementations typically hold onto some context (state) about the dataset being requested, so that when callers use
 * this handle to invoke other metadata API, more expensive operations can be performed.
 * <p>
 * For example, the entity path will be held as state.
 * <p>
 * Another example, when iterating over dataset handles using {@link SupportsListingDatasets#listDatasetHandles list handles
 * API}, a concrete implementation may hold onto a table name and driver connection (in turn, held inside the iterator),
 * and on invoking {@link SourceMetadata#getDatasetMetadata dataset metadata API}, the state in the handle is
 * used to get {@link DatasetStats} about that dataset from the source.
 * <p>
 * Note that the state held by implementations must have a small memory footprint.
 */
public interface DatasetHandle extends Unwrappable {

  /**
   * Get the path to the dataset.
   *
   * @return path to dataset, not null
   */
  EntityPath getDatasetPath();

}
