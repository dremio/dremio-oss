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

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;

import com.dremio.catalog.model.dataset.TableVersionContext;

/**
 * Provides support for querying tables using table function as following:
 * select * from table(table_history('iceberg_table'))
 * select * from table(table_manifests('iceberg_table'))
 * select * from table(table_snapshot('iceberg_table'))
 * select * from table(table_files('iceberg_table'))
 * select * from table(table_partitions('iceberg_table'))
 * will be translated into a call to this table macro, with the parsed TableVersionContext passed as a
 * parameter to apply().  The parsed table-id will be provided as a string in the 1st parameter to the macro.
 */
public class MetadataFunctionsMacro extends VersionedTableMacro {

  public enum MacroName {
    TABLE_HISTORY("table_history"),
    TABLE_MANIFESTS("table_manifests"),
    TABLE_SNAPSHOT("table_snapshot"),
    TABLE_FILES("table_files"),
    TABLE_PARTITIONS("table_partitions");
    private final String name;
    MacroName(String name) {
      this.name = name;
    }
    public String getName() {
      return name;
    }
  }

  private final TranslatableTableResolver tableResolver;
  private static final List<FunctionParameter> FUNCTION_PARAMS = new ReflectiveFunctionBase.ParameterListBuilder()
    .add(String.class, "table_name").build();

  public MetadataFunctionsMacro(TranslatableTableResolver tableResolver) {
    this.tableResolver = tableResolver;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return FUNCTION_PARAMS;
  }

  @Override
  public TranslatableTable apply(final List<? extends Object> arguments, TableVersionContext tableVersionContext) {
    final List<String> tablePath = splitTableIdentifier((String) arguments.get(0));
    return tableResolver.find(tablePath, tableVersionContext);
  }
}
