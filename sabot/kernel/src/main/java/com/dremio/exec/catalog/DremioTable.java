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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * A TranslatableTable (View or Table) that also knows its canonical name and exposes schema information.
 */
public interface DremioTable extends TranslatableTable {

  /**
   * Canonical path of the table. Note that this may be different than what was requested (both in
   * casing and components) depending on the behavior of the underlying source.
   *
   * @return
   */
  NamespaceKey getPath();

  /**
   * Provide the version of the dataset, if available. Otherwise, return -1.
   * @return
   */
  String getVersion();

  /**
   * The BatchSchema for the dataset. For the exception of old dot file views, this returns correct
   * schema according to sampling/metadata of the underlying system.
   *
   * @return BatchSchema for the dataset.
   */
  BatchSchema getSchema();

  DatasetConfig getDatasetConfig();

  default boolean isRolledUp(String column) {
    return false;
  }

  default boolean rolledUpColumnValidInsideAgg(
      String column,
      SqlCall call,
      SqlNode parent,
      CalciteConnectionConfig config) {
    return true;
  }
}
