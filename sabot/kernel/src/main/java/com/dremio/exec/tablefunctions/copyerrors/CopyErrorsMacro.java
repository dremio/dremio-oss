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

import java.util.List;

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;

import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.handlers.query.CopyErrorContext;

/**
 * {@link org.apache.calcite.schema.TableMacro} implementation entry point for copy_errors table function.
 */
public final class CopyErrorsMacro implements TableMacro {

  public static final String MACRO_NAME = "copy_errors";
  private final SimpleCatalog<?> catalog;
  static final List<FunctionParameter> FUNCTION_PARAMETERS = new ReflectiveFunctionBase.ParameterListBuilder()
    .add(String.class, "table_name")
    .add(String.class, "job_id", true)
    .add(Boolean.class, "strict_consistency", true)
    .build();

  public CopyErrorsMacro(SimpleCatalog<?> catalog) {
    this.catalog = catalog;
  }

  @Override
  public TranslatableTable apply(List<?> arguments) {
    // by default we enforce consistency check
    boolean strictConsistency = arguments.get(2) != null ? (Boolean) arguments.get(2) : true;

    CopyErrorContext context = new CopyErrorContext(catalog,
      (String) arguments.get(0),
      (String) arguments.get(1),
      strictConsistency
    );
    return new CopyErrorsTranslatableTable(context);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return FUNCTION_PARAMETERS;
  }

}
