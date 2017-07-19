/*
 * Copyright 2016 Dremio Corporation
 */
 package com.dremio.exec.planner.sql.parser;

import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.planner.sql.VarArgSqlOperator;

public class SqlContains {

  private static final MajorType RETURN_TYPE = Types.required(MinorType.BIT);

  private static final VarArgSqlOperator OPERATOR = new VarArgSqlOperator("contains", RETURN_TYPE, true);

  public static SqlNode getNode(SqlParserPos pos, Map<String, String> fieldMap, String queryString) {
    SqlNode[] operands = new SqlNode[fieldMap.size() + 1];
    for (String field : fieldMap.keySet()) {
      int index = Integer.parseInt(fieldMap.get(field).substring(1));
      operands[index] = getIdentifier(field);
    }
    SqlNode query = SqlLiteral.createCharString(queryString, pos);
    operands[operands.length - 1] = query;
    return new SqlBasicCall(OPERATOR, operands, pos);
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
