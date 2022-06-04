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

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.tablefunctions.VersionedTableMacro;

/**
 * Implementation of {@link SqlUserDefinedTableMacro} which wraps a {@link VersionedTableMacro} instead of a normal
 * TableMacro.  Version information, in the form of a {@link TableVersionContext}, is retrieved from the parent
 * {@link SqlVersionedTableMacroCall} and then passed to {@link VersionedTableMacro#apply(List, TableVersionContext)}.
 */
public class SqlVersionedTableMacro extends SqlUserDefinedTableMacro {

  private final VersionedTableMacro tableMacro;
  private TableVersionContext tableVersionContext = TableVersionContext.LATEST_VERSION;

  public SqlVersionedTableMacro(SqlIdentifier opName, SqlReturnTypeInference returnTypeInference,
                                SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
                                List<RelDataType> paramTypes, VersionedTableMacro tableMacro) {
    super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, tableMacro);
    this.tableMacro = tableMacro;
  }

  @Override
  protected void preValidateCall(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    if (call instanceof SqlVersionedTableMacroCall) {
      SqlVersionedTableMacroCall versionedTableMacroCall = (SqlVersionedTableMacroCall) call;
      tableVersionContext = versionedTableMacroCall.getResolvedTableVersionContext();
    }
  }

  @Override
  public TranslatableTable getTable(RelDataTypeFactory typeFactory,
                                    List<SqlNode> operandList) {
    List<Object> arguments = convertArguments(typeFactory, operandList, tableMacro, getNameAsId(), true);
    return tableMacro.apply(arguments, tableVersionContext);
  }
}
