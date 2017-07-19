/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.physical.base;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.vector.complex.fn.FieldSelection;

public abstract class SubScanWithProjection extends AbstractSubScan {
  private List<SchemaPath> columns;

  public SubScanWithProjection(String userName, BatchSchema schema, List<String> tablePath, List<SchemaPath> columns) {
    super(userName, schema, tablePath);
    this.columns = columns;
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    return getSchema().maskAndReorder(columns);
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }


}
