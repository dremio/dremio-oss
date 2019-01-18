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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;

import com.google.common.base.Objects;

public class SqlFlattenOperator extends SqlFunction {
  public static final SqlFlattenOperator INSTANCE = new SqlFlattenOperator(0);

  private int index;

  public SqlFlattenOperator(int index) {
    super(new SqlIdentifier("FLATTEN", SqlParserPos.ZERO), DynamicReturnType.INSTANCE, null, OperandTypes.ANY, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.index = index;
  }

  public SqlFlattenOperator withIndex(int index){
    return new SqlFlattenOperator(index);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SqlFlattenOperator)) {
      return false;
    }
    SqlFlattenOperator castOther = (SqlFlattenOperator) other;
    return Objects.equal(index, castOther.index);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(index);
  }

  public int getIndex(){
    return index;
  }

}
