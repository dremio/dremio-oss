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
package com.dremio.exec.catalog;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.service.catalog.Table;
import com.dremio.service.namespace.NamespaceKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Interface used to read datasets and containers. */
public interface EntityExplorer {

  /**
   * Retrieve a table ignoring the default schema.
   *
   * @param key
   * @return A DremioTable if found, otherwise null.
   */
  DremioTable getTableNoResolve(NamespaceKey key);

  /**
   * Retrieve a table snapshot ignoring the default schema.
   *
   * @param catalogEntityKey
   * @return A DremioTable if found, otherwise null.
   */
  DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey);

  /**
   * Retrieve a table ignoring column count.
   *
   * @param key
   * @return A DremioTable if found, otherwise null.
   */
  DremioTable getTableNoColumnCount(NamespaceKey key);

  /**
   * Determine whether the container at the given path exists. Note that this first looks to see if
   * the container exists directly via a lookup. However, in the case of sources, we have to do two
   * additional checks. First, we have to check if there is a physical dataset in the path (because
   * in FileSystems, sometimes the folders leading to a dataset don't exist). If that returns false,
   * we finally have to consult the source directly to see if the path exists.
   *
   * @param path Container path to check.
   * @return True if the path exists and is readable by the user. False if it doesn't exist.
   */
  boolean containerExists(CatalogEntityKey path);

  /**
   * Get a list of all schemas.
   *
   * @param path The path to contextualize to. If the path has no fields, get all schemas. Note that
   *     this does include nested schemas.
   * @return Iterable list of strings of each schema.
   */
  Iterable<String> listSchemas(NamespaceKey path);

  /**
   * Get a list of all tables available in this catalog within the provided path.
   *
   * @param path The path to constraint the listing to.
   * @return The set of tables within the provided path.
   */
  Iterable<Table> listDatasets(NamespaceKey path);

  /**
   * Retrieve a table's definition
   *
   * @param datasetId
   * @return
   */
  DremioTable getTable(String datasetId);

  /**
   * Retrieve a table's definition, first checking the default schema.
   *
   * @param key
   * @return
   */
  DremioTable getTable(NamespaceKey key);

  // TODO(DX-83444) Consolidate with getTable
  /**
   * Retrieve a table when querying the table's data, first checking the default schema
   *
   * @param key
   * @return
   */
  DremioTable getTableForQuery(NamespaceKey key);

  // TODO(DX-83444) Consolidate with getTableSnapshot
  /**
   * Retrieve a table snapshot when querying the table's data, first checking the default schema
   *
   * @param catalogEntityKey path to table
   * @return DremioTable
   */
  DremioTable getTableSnapshotForQuery(CatalogEntityKey catalogEntityKey);

  /**
   * Retrieve a table snapshot when querying the table's data
   *
   * @param catalogEntityKey@return DremioTable
   */
  DremioTable getTableSnapshot(CatalogEntityKey catalogEntityKey);

  /**
   * Retrieve the column extended properties for a table.
   *
   * @param table the table to get the column extended properties for
   * @return the column extended properties grouped by column name
   */
  Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table);

  /**
   * Process an ad-hoc metadata verify request of {@link TableMetadataVerifyRequest} on a table and
   * return the result of {@link TableMetadataVerifyResult}.
   *
   * @param catalogEntityKey for table
   * @param metadataVerifyRequest metadata verify request
   * @return metadata verify result
   */
  @Nonnull
  Optional<TableMetadataVerifyResult> verifyTableMetadata(
      CatalogEntityKey catalogEntityKey, TableMetadataVerifyRequest metadataVerifyRequest);

  DremioTable getTable(CatalogEntityKey catalogEntityKey);
}
