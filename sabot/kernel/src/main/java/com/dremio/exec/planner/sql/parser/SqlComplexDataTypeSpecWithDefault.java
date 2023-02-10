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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

/**
 * Extends SqlComplexDataTypeSpec to support default expression.
 */
public class SqlComplexDataTypeSpecWithDefault extends SqlComplexDataTypeSpec {
  private final SqlNode defaultExpression;

  public SqlComplexDataTypeSpecWithDefault(SqlDataTypeSpec spec, SqlNode defaultExpression) {
    super(spec);
    this.defaultExpression = defaultExpression;
  }

  public SqlNode getDefaultExpression() {
    return defaultExpression;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (defaultExpression != null) {
      writer.keyword("DEFAULT");
      defaultExpression.unparse(writer, leftPrec, rightPrec);
    }
  }
}
