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
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;

public abstract class SubScanWithProjection extends AbstractSubScan {
  private List<SchemaPath> columns;

  public SubScanWithProjection(
      OpProps props,
      BatchSchema fullSchema,
      Collection<List<String>> referencedTables,
      List<SchemaPath> columns) {
    super(props, fullSchema, referencedTables);
    this.columns = columns;
  }

  public SubScanWithProjection(
      OpProps props,
      BatchSchema fullSchema,
      List<String> referencedTable,
      List<SchemaPath> columns) {
    super(props, fullSchema, referencedTable == null ? null : ImmutableList.of(referencedTable));
    this.columns = columns;
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

}
