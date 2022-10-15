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
package com.dremio.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import com.dremio.service.namespace.NamespaceKey;

/**
 * Interface to be implemented for three DML operations (at the moment)
 * which include DELETE, UPDATE, and MERGE.
 */
public interface SqlDmlOperator {

  /**
   * Return the table the DMLs will impact/target.
   */
  SqlNode getTargetTable();

  /**
   * Adds an `EXTEND` with relevant system columns, currently only used
   * to add columns for Iceberg.
   */
  void extendTableWithDataFileSystemColumns();

  /**
   *  Check if the table has been extended by calling extendTableWithDataFileSystemColumns()
   */
  default boolean isTableExtended() {
    SqlNode targetTable = getTargetTable();
    return targetTable instanceof SqlCall && targetTable.getKind() == SqlKind.EXTEND;
  }

  /**
   * Get the table path name.
   */
  default NamespaceKey getPath() {
    return DmlUtils.getPath(getTargetTable());
  }

  /**
   *  Get the source of DML operation.
   */
  SqlNode getSourceTableRef();

  /**
   * @return the alias for the target table
   */
  SqlIdentifier getAlias();

  /**
   *
   * @return the condition expression for the DMLed data
   */
  SqlNode getCondition();
}
