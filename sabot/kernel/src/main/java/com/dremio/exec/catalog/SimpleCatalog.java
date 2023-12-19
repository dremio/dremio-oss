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
import java.util.Map;

import org.apache.calcite.schema.Function;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Simplified catalog object needed for use with PlannerCatalog. A simplified version of Catalog.
 *
 * APIs needed for planning that aren't covered by the other specialized Catalog interfaces and aren't needed
 * for the REST or UI should go here.
 */
public interface SimpleCatalog<T extends SimpleCatalog<T>> extends EntityExplorer {

  /**
   * Get a list of functions. Provided specifically for DremioCatalogReader.
   * @param path
   * @param functionType
   * @return
   */
  Collection<Function> getFunctions(NamespaceKey path,
    FunctionType functionType);

  /**
   * Get the default schema for this catalog. Returns null if there is no default.
   * @return The default schema path.
   */
  NamespaceKey getDefaultSchema();

  /**
   * Return a new Catalog contextualized to the provided default schema
   * @param newDefaultSchema
   * @return A new schema with the same user but with the newly provided default schema.
   */
  T resolveCatalog(NamespaceKey newDefaultSchema);

  /**
   * Return a new Catalog contextualized to the provided version context
   * @param  sourceVersionMapping
   * @return A new catalog contextualized to the provided version context.
   */
  T resolveCatalog(Map<String, VersionContext> sourceVersionMapping);

  /**
   * Visits each catalog in a depth first order.
   * @param catalogRewrite function for transforming the catalog
   * @return resulting transformed catalog
   */
  default Catalog visit(java.util.function.Function<Catalog, Catalog> catalogRewrite) {throw new UnsupportedOperationException();}

  /**
   * @return all tables that have been requested from this catalog.
   */
  default Iterable<DremioTable> getAllRequestedTables() {throw new UnsupportedOperationException();}

  /**
   * Clears all caches associated to a particular dataset
   * @param dataset
   */
  default void clearDatasetCache(final NamespaceKey dataset, final TableVersionContext context) {};

  enum FunctionType {
    SCALAR,
    TABLE
  }
}
