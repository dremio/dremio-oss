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
package com.dremio.exec.tablefunctions;

import java.util.List;
import java.util.function.Function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase.ParameterListBuilder;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;

/**
 * TableMacro implementation for the external_query table function.
 */
public final class ExternalQuery implements TableMacro {
  private static final List<FunctionParameter> FUNCTION_PARAMS = new ParameterListBuilder()
    .add(String.class, "query").build();

  private final Function<String, BatchSchema> schemaBuilder;
  private final Function<BatchSchema, RelDataType> rowTypeBuilder;
  private final StoragePluginId pluginId;

  public ExternalQuery(Function<String, BatchSchema> schemaBuilder,
                       Function<BatchSchema, RelDataType> rowTypeBuilder,
                       StoragePluginId pluginId) {
    this.schemaBuilder = schemaBuilder;
    this.rowTypeBuilder = rowTypeBuilder;
    this.pluginId = pluginId;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return FUNCTION_PARAMS;
  }

  @Override
  public TranslatableTable apply(List<? extends Object> arguments) {
    return ExternalQueryTranslatableTable.create(schemaBuilder, rowTypeBuilder, pluginId, (String) arguments.get(0));
  }
}
