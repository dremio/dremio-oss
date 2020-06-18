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

import java.util.Iterator;

import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.View;

/**
 * Facet of the catalog service that provides metadata with an information_schema like API.
 * <p>
 * The metadata provided may not satisfy the configured metadata policy requirement of various sources; the reads
 * are directly from the KVStore without an inline refresh of the metadata.
 */
interface InformationSchemaCatalog {

  /**
   * List catalogs that satisfy the given search query.
   *
   * @param searchQuery        search query, may be null
   * @return iterator of catalogs
   */
  Iterator<Catalog> listCatalogs(SearchQuery searchQuery);

  /**
   * List schemata that satisfy the given search query.
   *
   * @param searchQuery        search query, may be null
   * @return iterator of schemata
   */
  Iterator<Schema> listSchemata(SearchQuery searchQuery);

  /**
   * List tables (datasets) that satisfy the given search query.
   *
   * @param searchQuery        search query, may be null
   * @return iterator of tables
   */
  Iterator<Table> listTables(SearchQuery searchQuery);

  /**
   * List views that satisfy the given search query.
   *
   * @param searchQuery        search query, may be null
   * @return iterator of views
   */
  Iterator<View> listViews(SearchQuery searchQuery);

  /**
   * List columns of all datasets that satisfy the given search query.
   *
   * @param searchQuery        search query, may be null
   * @return iterator of columns
   */
  Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery);

}
