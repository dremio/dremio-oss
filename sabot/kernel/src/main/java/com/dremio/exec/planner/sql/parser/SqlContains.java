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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.CONTAINS_OPERATOR;

import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;

public class SqlContains {
  public static final MajorType RETURN_TYPE = Types.required(MinorType.BIT);

  public static SqlNode getNode(SqlParserPos pos, Map<String, String> fieldMap, String queryString) {
    SqlNode[] operands = new SqlNode[fieldMap.size() + 1];
    for (String field : fieldMap.keySet()) {
      int index = Integer.parseInt(fieldMap.get(field).substring(1));
      operands[index] = getIdentifier(field);
    }
    SqlNode query = SqlLiteral.createCharString(queryString, pos);
    operands[operands.length - 1] = query;
    return new SqlBasicCall(CONTAINS_OPERATOR, operands, pos);
  }

  private static CompoundIdentifier getIdentifier(String field) {
    String[] segments = field.split("\\.");
    CompoundIdentifier.Builder builder = CompoundIdentifier.newBuilder();
    for (String segment : segments) {
      builder.addString(segment, false, SqlParserPos.ZERO);
    }
    return builder.build();
  }

  public static String getNewField(String field, Map<String, String> fieldMap) {
    String newField = fieldMap.get(field);
    if (newField == null) {
      newField = String.format("$%d", fieldMap.keySet().size());
      fieldMap.put(field, newField);
    }
    return newField;
  }
}
