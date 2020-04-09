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

import com.dremio.exec.store.ischema.tables.TablesTable;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Interface used to read datasets and containers.
 */
public interface EntityExplorer {

  /**
   * Retrieve a table ignoring the default schema.
   *
   * @param key
   * @return A DremioTable if found, otherwise null.
   */
  DremioTable getTableNoResolve(NamespaceKey key);

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
  boolean containerExists(NamespaceKey path);

  /**
   * Get a list of all schemas.
   *
   * @param path
   *          The path to contextualize to. If the path has no fields, get all schemas. Note
   *          that this does include nested schemas.
   * @return Iterable list of strings of each schema.
   */
  Iterable<String> listSchemas(NamespaceKey path);

  /**
   * Get a list of all tables available in this catalog within the provided path.
   * @param path The path to constraint the listing to.
   * @return The set of tables within the provided path.
   */
  Iterable<TablesTable.Table> listDatasets(NamespaceKey path);

  /**
   * Retrieve a table
   *
   * @param datasetId
   * @return
   */
  DremioTable getTable(String datasetId);

  /**
   * Retrieve a table, first checking the default schema.
   *
   * @param key
   * @return
   */
  DremioTable getTable(NamespaceKey key);
}
