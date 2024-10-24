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
package com.dremio.plugins.icebergcatalog.store;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import java.util.List;
import org.apache.iceberg.TableMetadata;

/** Provides interface between @IcebergCatalogPlugin and concrete @Catalog implementation */
public interface CatalogAccessor extends AutoCloseable {
  /** Get current state for catalog accessor. */
  void checkState() throws Exception;

  /**
   * Returns a listing of dataset handles, where each handle represents a dataset in the source.
   *
   * @param rootName root name of a dataset, usually plugin name
   * @param plugin an Iceberg capable plugin instance where this request originates from
   * @return listing of dataset handles
   */
  DatasetHandleListing listDatasetHandles(String rootName, SupportsIcebergRootPointer plugin);

  /**
   * Given a dataset , return a handle that represents the dataset.
   *
   * @param dataset table path
   * @param plugin an Iceberg capable plugin instance where this request originates from
   * @param options options
   * @return an optional dataset handle, not null
   */
  DatasetHandle getDatasetHandle(
      List<String> dataset, SupportsIcebergRootPointer plugin, GetDatasetOption... options);

  /**
   * Check if the given dataset exists and represents a table
   *
   * @param dataset table path
   * @return true if the given table exists
   */
  boolean datasetExists(List<String> dataset);

  /**
   * Loads Iceberg Table Metadata for Iceberg Catalog table
   *
   * @param dataset table path
   * @return Table Metadata for the given dataset
   */
  TableMetadata getTableMetadata(List<String> dataset);
}
