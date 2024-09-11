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
package com.dremio.exec.store.iceberg.dremioudf.api.catalog;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfBuilder;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

/** A Catalog API for UDF create, drop, and load operations. */
public interface UdfCatalog {

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  String name();

  /**
   * Return all the identifiers under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for UDFs
   * @throws NoSuchNamespaceException if the namespace is not found
   */
  List<TableIdentifier> listUdfs(Namespace namespace);

  /**
   * Load a UDF.
   *
   * @param identifier a UDF identifier
   * @return instance of {@link Udf} implementation referred by the identifier
   * @throws NoSuchUdfException if the UDF does not exist
   */
  Udf loadUdf(TableIdentifier identifier);

  /**
   * Check whether UDF exists.
   *
   * @param identifier a UDF identifier
   * @return true if the UDF exists, false otherwise
   */
  default boolean udfExists(TableIdentifier identifier) {
    try {
      loadUdf(identifier);
      return true;
    } catch (NoSuchUdfException e) {
      return false;
    }
  }

  /**
   * Instantiate a builder to create or replace a SQL UDF.
   *
   * @param identifier a UDF identifier
   * @return a UDF builder
   */
  UdfBuilder buildUdf(TableIdentifier identifier);

  /**
   * Drop a UDF.
   *
   * @param identifier a UDF identifier
   * @return true if the UDF was dropped, false if the UDF did not exist
   */
  boolean dropUdf(TableIdentifier identifier);

  /**
   * Rename a UDF.
   *
   * @param from identifier of the UDF to rename
   * @param to new UDF identifier
   * @throws NoSuchUdfException if the "from" UDF does not exist
   * @throws AlreadyExistsException if the "to" UDF already exists
   * @throws NoSuchNamespaceException if the "to" namespace doesn't exist
   */
  void renameUdf(TableIdentifier from, TableIdentifier to);

  /**
   * Invalidate cached UDF metadata from current catalog.
   *
   * <p>If the UDF is already loaded or cached, drop cached data. If the UDF does not exist or is
   * not cached, do nothing.
   *
   * @param identifier a UDF identifier
   */
  default void invalidateUdf(TableIdentifier identifier) {}

  /**
   * Initialize a UDF catalog given a custom name and a map of catalog properties.
   *
   * <p>A custom UDF catalog implementation must have a no-arg constructor. A compute engine like
   * Spark or Flink will first initialize the catalog without any arguments, and then call this
   * method to complete catalog initialization with properties passed into the engine.
   *
   * @param name a custom name for the catalog
   * @param properties catalog properties
   */
  default void initialize(String name, Map<String, String> properties) {}
}
