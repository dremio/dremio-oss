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

import com.dremio.exec.planner.sql.SqlVersionedIdentifier;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;

/**
 * UDF function call with version spec. E.g. catalog.udf1('a') AT BRANCH dev. The table version spec
 * is encoded in a {@link SqlVersionedIdentifier} object. The original SqlIdentifier in operator is
 * replaced with the SqlVersionedIdentifier object.
 */
public class SqlVersionedFunctionCall extends SqlBasicCall {

  private final SqlTableVersionSpec sqlTableVersionSpec;

  public SqlVersionedFunctionCall(SqlCall call, SqlTableVersionSpec sqlTableVersionSpec) {
    super(
        call.getOperator(),
        call.getOperandList().toArray(new SqlNode[call.getOperandList().size()]),
        call.getParserPosition());
    this.sqlTableVersionSpec = sqlTableVersionSpec;

    // Update operator to include SqlTableVersionSpec
    Preconditions.checkState(call.getOperator() instanceof SqlUnresolvedFunction);
    SqlUnresolvedFunction operator = (SqlUnresolvedFunction) call.getOperator();
    SqlVersionedIdentifier identifier =
        new SqlVersionedIdentifier(operator.getNameAsId(), sqlTableVersionSpec);
    SqlUnresolvedFunction operatorWithVersionContext =
        new SqlUnresolvedFunction(
            identifier,
            operator.getReturnTypeInference(),
            operator.getOperandTypeInference(),
            operator.getOperandTypeChecker(),
            operator.getParamTypes(),
            operator.getFunctionType());
    this.setOperator(operatorWithVersionContext);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    this.sqlTableVersionSpec.unparse(writer, leftPrec, rightPrec);
  }
}
