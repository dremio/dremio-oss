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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlAbstractStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;

/**
 * A character string literal.
 * Copied from Calcite's {@link SqlCharStringLiteral}, but with VARCHAR type
 *
 * <p>Its {@link #value} field is an {@link NlsString} and {@link #typeName} is
 * {@link SqlTypeName#VARCHAR}.
 */
public class SqlVarCharStringLiteral extends SqlAbstractStringLiteral {
  private static final Function<SqlLiteral, NlsString> F =
    literal -> ((SqlVarCharStringLiteral) literal).getNlsString();

  public static SqlVarCharStringLiteral create(SqlCharStringLiteral charStringLiteral) {
    return new SqlVarCharStringLiteral(charStringLiteral.getNlsString(), charStringLiteral.getParserPosition());
  }

  private SqlVarCharStringLiteral(NlsString val, SqlParserPos pos) {
    super(val, SqlTypeName.VARCHAR, pos);
  }

  /**
   * @return the underlying NlsString
   */
  public NlsString getNlsString() {
    return (NlsString) value;
  }

  /**
   * @return the collation
   */
  public SqlCollation getCollation() {
    return getNlsString().getCollation();
  }

  @Override public SqlVarCharStringLiteral clone(SqlParserPos pos) {
    return new SqlVarCharStringLiteral((NlsString) value, pos);
  }

  public void unparse(
    SqlWriter writer,
    int leftPrec,
    int rightPrec) {
    if (false) {
      Util.discard(Bug.FRG78_FIXED);
      String stringValue = ((NlsString) value).getValue();
      writer.literal(
        writer.getDialect().quoteStringLiteral(stringValue));
    }
    assert value instanceof NlsString;
    writer.literal(value.toString());
  }

  @Override
  protected SqlAbstractStringLiteral concat1(List<SqlLiteral> literals) {
    return new SqlVarCharStringLiteral(
      NlsString.concat(literals.stream().map(F::apply).collect(Collectors.toList())),
      literals.get(0).getParserPosition());
  }
}
