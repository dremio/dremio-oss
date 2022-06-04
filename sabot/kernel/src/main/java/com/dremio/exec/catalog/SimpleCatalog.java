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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Function;

import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Simplified catalog object needed for use with DremioCatalogReader. A simplified version of Catalog.
 */
public interface SimpleCatalog<T extends SimpleCatalog<T>> extends EntityExplorer {

  /**
   * Get a list of functions. Provided specifically for DremioCatalogReader.
   * @param path
   * @return
   */
  Collection<Function> getFunctions(NamespaceKey path);

  /**
   * Get the default schema for this catalog. Returns null if there is no default.
   * @return The default schema path.
   */
  NamespaceKey getDefaultSchema();

  /**
   * Return a new Catalog contextualized to the validity check flag
   *
   * @param checkValidity
   * @return
   */
  T resolveCatalog(boolean checkValidity);

  /**
   * Return a new Catalog contextualized to the provided subject and default schema
   *
   * @param subject
   * @param newDefaultSchema
   * @return
   */
  T resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema);

  /**
   * Return a new Catalog contextualized to the provided subject, default schema, and validity check flag
   *
   * @param subject
   * @param newDefaultSchema
   * @param checkValidity
   * @return
   */
  T resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity);

  /**
   * Return a new Catalog contextualized to the provided default schema
   * @param newDefaultSchema
   * @return A new schema with the same user but with the newly provided default schema.
   */
  T resolveCatalog(NamespaceKey newDefaultSchema);

  /**
   * Validate if there is a violation of cross source selection
   */
  void validateSelection();

  /**
   * Retrieve the column extended properties for a table.
   * @param table the table to get the column extended properties for
   * @return the column extended properties grouped by column name
   */
  Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table);
}
