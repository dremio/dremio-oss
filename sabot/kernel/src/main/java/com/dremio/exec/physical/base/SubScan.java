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
package com.dremio.exec.physical.base;

import java.util.Collection;
import java.util.List;

import com.dremio.exec.record.BatchSchema;

/**
 * A SubScan operator represents the data scanned by a particular major/minor fragment.  This is in contrast to
 * a GroupScan operator, which represents all data scanned by a physical plan.
 */
public interface SubScan extends Scan {

  /**
   * Get the list of schema paths of tables referenced by this Scan. Each table's schema path, in the namespace
   * hierarchy, is a list of strings. For example ["mysql", "database", "table"].
   *
   * @return list of referenced tables
   */
  Collection<List<String>> getReferencedTables();

  /**
   * If schema of referenced tables may be learnt in case of schema changes.
   */
  boolean mayLearnSchema();

  /**
   * Return the complete table schema. This schema includes fields, not just
   * those projected. It differs from the getSchema(FunctionLookupContext) as
   * that one only includes the fields that are projected. Use this for schema
   * leraning updates and the other one for output schema determination.
   *
   * @return The full schema of the table.
   */
  BatchSchema getFullSchema();

}
