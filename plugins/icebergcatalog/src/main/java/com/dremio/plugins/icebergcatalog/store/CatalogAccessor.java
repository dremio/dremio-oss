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
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;

/** Provides interface between @IcebergCatalogPlugin and concrete @Catalog implementation */
public interface CatalogAccessor
    extends SupportsIcebergDatasetCUD, SupportsIcebergFolderCUD, AutoCloseable {
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
   * Given a dataset, return a handle that represents the dataset.
   *
   * @param dataset table path
   * @param plugin an Iceberg capable plugin instance where this request originates from
   * @param options options
   * @return an optional dataset handle, not null
   */
  Optional<DatasetHandle> getDatasetHandle(
      List<String> dataset, SupportsIcebergRootPointer plugin, GetDatasetOption... options);

  /**
   * Check if the given dataset exists and represents a table
   *
   * @param dataset table path
   * @return true if the given table exists
   * @throws BadRequestException if the path to the dataset is wrong.
   */
  boolean datasetExists(List<String> dataset) throws BadRequestException;

  /**
   * Loads Iceberg Table Metadata for Iceberg Catalog table
   *
   * @param dataset table path
   * @return Table Metadata for the given dataset
   */
  TableMetadata getTableMetadata(List<String> dataset);

  /**
   * Loads (or gets from cache) the ViewMetadata for an Iceberg Catalog View .
   *
   * @param dataset view path
   * @return
   */
  ViewMetadata getViewMetadata(List<String> dataset);

  /**
   * Returns a listing of partition chunk handles for the given dataset handle.
   *
   * @param icebergTableProvider IcebergTable provider
   * @param options options
   * @return listing of partition chunk handles, not null
   */
  PartitionChunkListing listPartitionChunks(
      IcebergCatalogTableProvider icebergTableProvider, ListPartitionChunkOption[] options);

  /**
   * Returns the DatasetMetadata for the given dataset handle.
   *
   * @param icebergTableProvider IcebergTable provider
   * @param options options
   * @return dataset metadata
   */
  DatasetMetadata getTableMetadata(
      IcebergCatalogTableProvider icebergTableProvider, GetMetadataOption[] options);

  /**
   * Returns the DatasetMetadata for the given view dataset handle.
   *
   * @param viewDatasetHandle view dataset handle
   * @return view metadata
   */
  DatasetMetadata getViewMetadata(DatasetHandle viewDatasetHandle);

  /**
   * Check if the given namespace exists as a namespace
   *
   * @param namespace Namespace whose existence to check
   * @return whether namespace exists
   */
  boolean namespaceExists(List<String> namespace);

  /**
   * Returns a listing of dataset identifiers (i.e. their paths) starting at a given path
   *
   * @param pathWithSourceName path to list from, including source name
   * @return listing of dataset handles
   */
  Set<TableIdentifier> listDatasetIdentifiers(List<String> pathWithSourceName);

  Stream<IcebergNamespaceWithProperties> getFolderStream();

  String getDefaultBaseLocation();

  Transaction createTableTransactionForNewTable(TableIdentifier tableIdentifier, Schema schema);

  View loadView(TableIdentifier tableIdentifier, GetDatasetOption... options);

  /**
   * Returns the properties of the table's FileIO as the catalog configures it for the table: the
   * catalog properties merged with the table config of the catalog's response, which carries the
   * storage credentials the catalog vends (Iceberg REST access delegation {@code
   * vended-credentials}). The values may be secrets: never log them.
   *
   * @param tableIdentifier the table
   * @param stageNewTable what to return if the table does not exist yet (e.g. while CTAS writes its
   *     data files): if true, the properties of a staged creation of the table at its default
   *     location (nothing is committed); if false, none
   * @return the properties, empty if the catalog does not provide any
   */
  default Map<String, String> loadTableStorageProperties(
      TableIdentifier tableIdentifier, boolean stageNewTable) {
    return Collections.emptyMap();
  }
}
