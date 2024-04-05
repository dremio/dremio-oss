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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * Supports struct< x: bigint, y : bigint > type of struct schema
 *
 * <p>We also support to add a [ NULL | NOT NULL ] suffix for every field type
 */
public final class DremioSqlRowTypeSpec extends SqlTypeNameSpec {

  private final List<SqlIdentifier> fieldNames;
  private final List<SqlComplexDataTypeSpec> fieldTypes;

  /**
   * Creates a row type specification.
   *
   * @param pos The parser position.
   * @param fieldNames The field names.
   * @param fieldTypes The field data types.
   */
  public DremioSqlRowTypeSpec(
      SqlParserPos pos, List<SqlIdentifier> fieldNames, List<SqlComplexDataTypeSpec> fieldTypes) {
    super(SqlTypeName.ROW.getName(), pos);
    Objects.requireNonNull(fieldNames);
    Objects.requireNonNull(fieldTypes);
    this.fieldNames = ImmutableList.copyOf(fieldNames);
    this.fieldTypes = ImmutableList.copyOf(fieldTypes);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print(SqlTypeName.ROW.getName());
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (Pair<SqlIdentifier, SqlComplexDataTypeSpec> p :
        Pair.zip(this.fieldNames, this.fieldTypes)) {
      writer.sep(",", false);
      p.left.unparse(writer, 0, 0);
      p.right.unparse(writer, leftPrec, rightPrec);
      if (p.right.getNullable() != null && p.right.getNullable()) {
        writer.print("NULL");
      }
    }
    writer.endList(frame);
  }

  @Override
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(
        fieldTypes.stream().map(dt -> dt.deriveType(typeFactory)).collect(Collectors.toList()),
        fieldNames.stream().map(SqlIdentifier::toString).collect(Collectors.toList()));
  }

  public List<SqlIdentifier> getFieldNames() {
    return fieldNames;
  }

  public List<SqlComplexDataTypeSpec> getFieldTypes() {
    return fieldTypes;
  }
}
