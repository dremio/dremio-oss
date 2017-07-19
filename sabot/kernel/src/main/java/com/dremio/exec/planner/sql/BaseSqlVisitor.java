/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;

/**
 * BaseVisitor that throws exceptions by default
 * @param <T> return type
 */
public abstract class BaseSqlVisitor<T> implements SqlVisitor<T> {
  @Override
  public T visit(SqlLiteral lit) {
    throw new UnsupportedOperationException("SqlLiteral " + lit);
  }
  @Override
  public T visit(SqlCall call) {
    throw new UnsupportedOperationException("SqlCall " + call);
  }
  @Override
  public T visit(SqlNodeList list) {
    throw new UnsupportedOperationException("SqlNodeList " + list);
  }
  @Override
  public T visit(SqlIdentifier id) {
    throw new UnsupportedOperationException("SqlIdentifier " + id);
  }
  @Override
  public T visit(SqlDataTypeSpec spec) {
    throw new UnsupportedOperationException("SqlDataTypeSpec " + spec);
  }
  @Override
  public T visit(SqlDynamicParam param) {
    throw new UnsupportedOperationException("SqlDynamicParam " + param);
  }
  @Override
  public T visit(SqlIntervalQualifier qualifier) {
    throw new UnsupportedOperationException("SqlIntervalQualifier " + qualifier);
  }
}
