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
package com.dremio.exec.tablefunctions.copyerrors;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.record.BatchSchema;

/**
 * Copy Errors TableMetadata interface. This is how copy errors table is exposed to the planning environment.
 */
public final class CopyErrorsCatalogMetadata {
  private final BatchSchema schema;

  private final RelDataType rowType;

  public CopyErrorsCatalogMetadata(BatchSchema schema, RelDataType rowType) {
    this.schema = schema;
    this.rowType = rowType;
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public RelDataType getRowType() {
    return rowType;
  }
}
