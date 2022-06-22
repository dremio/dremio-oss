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

import com.dremio.exec.catalog.TableVersionContext;

/**
 * Provides support for querying tables at a specific version or point in time.  Table references of the form
 *
 *   table-id AT version-spec
 *
 * will be translated into a call to this table macro, with the parsed TableVersionContext passed as a
 * parameter to apply().  The parsed table-id will be provided as a string in the 1st parameter to the macro.
 */
public class TimeTravelTableMacro extends VersionedTableMacro {

  public static final String NAME = "time_travel";

  private final TranslatableTableResolver tableResolver;

  private static final List<FunctionParameter> FUNCTION_PARAMS = new ReflectiveFunctionBase.ParameterListBuilder()
      .add(String.class, "table_name").build();

  public TimeTravelTableMacro(TranslatableTableResolver tableResolver) {
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
