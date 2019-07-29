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
package com.dremio.jdbc.impl;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;

/**
 * Access to metadata
 */
public interface DremioMeta {
  /** Per {@link java.sql.DatabaseMetaData#getCatalogs()}. */
  MetaResultSet getCatalogs() throws SQLException;

  /** Per {@link java.sql.DatabaseMetaData#getSchemas(String, String)}. */
  MetaResultSet getSchemas(String catalog,
      Pat schemaPattern) throws SQLException;

  /** Per {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}. */
  MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList) throws SQLException;

  /** Per {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String)}. */
  MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) throws SQLException;

}
