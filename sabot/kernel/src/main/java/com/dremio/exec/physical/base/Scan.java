/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.physical.base;

import java.util.Collection;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonProperty;

public interface Scan extends Leaf {

  /**
   * Get the list of schema paths of tables referenced by this Scan. Each table's schema path, in the namespace
   * hierarchy, is a list of strings. For example ["mysql", "database", "table"].
   *
   * @return list of referenced tables
   */
  @JsonProperty("referenced-tables")
  Collection<List<String>> getReferencedTables();

  /**
   * If schema of referenced tables may be learnt in case of schema changes.
   */
  boolean mayLearnSchema();

  /**
   * Similar to base class functionality although FunctionLookupContext can be
   * null since this is a leaf node. The actual outcome schema.
   */
  @Override
  BatchSchema getSchema(FunctionLookupContext context);

  /**
   * Return the complete table schema. This schema includes fields, not just
   * those projected. It differs from the getSchema(FunctionLookupContext) as
   * that one only includes the fields that are projected. Use this for schema
   * leraning updates and the other one for output schema determination.
   *
   * @return The full schema of the table.
   */
  @JsonProperty("schema")
  BatchSchema getSchema();

  /**
   * The set of projected columns.
   *
   * @return The columns to be projected (they are applied to outcome of
   *         getSchema() to generate the projected schema.
   */
  @JsonProperty("columns")
  List<SchemaPath> getColumns();
}
