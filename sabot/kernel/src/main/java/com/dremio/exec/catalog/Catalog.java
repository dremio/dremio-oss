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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Interface used to retrieve virtual and physical datasets. This is always contextualized to a single user and
 * default schema. Implementations must be thread-safe
 */
public interface Catalog extends SimpleCatalog<Catalog>, EntityExplorer, DatasetCatalog, SourceCatalog, InformationSchemaCatalog {
  /**
   * @return all tables that have been requested from this catalog.
   */
  Iterable<DremioTable> getAllRequestedTables();

  /**
   * Resolve an ambiguous reference using the following rules: if the reference is a single value
   * and a default schema is defined, resolve using the default schema. Otherwise, resolve using the
   * name directly.
   *
   * @param key
   * @return
   */
  NamespaceKey resolveSingle(NamespaceKey key);

  /**
   * Resolve the provided key to the default schema path, if there is one.
   * @param key
   * @return
   */
  NamespaceKey resolveToDefault(NamespaceKey key);


  /**
   * Return a new Catalog contextualized to the provided username
   *
   * @param username
   * @return
   */
  Catalog resolveCatalog(String username);

  MetadataStatsCollector getMetadataStatsCollector();

  //TODO(DX-21034): Rework View Creator
  void createView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException;

  //TODO(DX-21034): Rework View Creator
  void updateView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException;

  //TODO(DX-21034): Rework View Creator
  void dropView(final NamespaceKey key) throws IOException;

  Iterable<String> getSubPartitions(NamespaceKey key, List<String> partitionColumns, List<String> partitionValues) throws PartitionNotFoundException;

  /**
   * Retrieve the column extended properties for a table.
   * @param table the table to get the column extended properties for
   * @return the column extended properties grouped by column name
   */
  Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table);

}
