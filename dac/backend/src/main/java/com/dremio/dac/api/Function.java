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
package com.dremio.dac.api;

import static com.dremio.exec.calcite.SqlNodes.DREMIO_DIALECT;

import com.dremio.common.expression.CompleteType;
import com.dremio.service.namespace.function.proto.FunctionArg;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.commons.collections4.CollectionUtils;

/** User Defined Function */
public class Function implements CatalogEntity {
  private final String id;
  private final List<String> path;
  private final String tag;
  @JsonISODateTime private final Long createdAt;
  @JsonISODateTime private final Long lastModified;
  private final Boolean isScalar;
  private final String functionArgList;
  private final String functionBody;
  private final String returnType;

  @JsonCreator
  public Function(
      @JsonProperty("id") String id,
      @JsonProperty("path") List<String> path,
      @JsonProperty("tag") String tag,
      @JsonProperty("createdAt") Long createdAt,
      @JsonProperty("lastModified") Long lastModified,
      @JsonProperty("isScalar") Boolean isScalar,
      @JsonProperty("functionArgList") String functionArgList,
      @JsonProperty("functionBody") String functionBody,
      @JsonProperty("returnType") String returnType) {
    this.id = id;
    this.path = path;
    this.tag = tag;
    this.createdAt = createdAt;
    this.lastModified = lastModified;
    this.isScalar = isScalar;
    this.functionBody = functionBody;
    this.functionArgList = functionArgList;
    this.returnType = returnType;
  }

  @JsonIgnore
  public String getName() {
    return Iterables.getLast(getPath());
  }

  @Override
  public String getId() {
    return id;
  }

  public List<String> getPath() {
    return path;
  }

  public String getTag() {
    return tag;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public Long getLastModified() {
    return lastModified;
  }

  public Boolean getIsScalar() {
    return isScalar;
  }

  public String getFunctionBody() {
    return functionBody;
  }

  public String getFunctionArgList() {
    return functionArgList;
  }

  public String getReturnType() {
    return returnType;
  }

  @Nullable
  @Override
  public String getNextPageToken() {
    return null;
  }

  public static Function fromFunctionConfig(FunctionConfig config) {
    // We assume here that stored function definitions are syntactically correct.
    FunctionDefinition functionDefinition = config.getFunctionDefinitionsList().get(0);
    CompleteType returnType =
        CompleteType.deserialize(config.getReturnType().getRawDataType().toByteArray());
    return new com.dremio.dac.api.Function(
        config.getId().getId(),
        config.getFullPathList(),
        config.getTag(),
        config.getCreatedAt(),
        config.getLastModified(),
        returnType.isScalar(),
        CollectionUtils.isEmpty(functionDefinition.getFunctionArgList())
            ? ""
            : functionArgListToString(functionDefinition.getFunctionArgList()),
        functionDefinition.getFunctionBody().getRawBody(),
        returnTypeToString(returnType));
  }

  private static String functionArgListToString(List<FunctionArg> functionArgList) {
    SqlWriter writer = getSqlWriter();
    for (int i = 0; i < functionArgList.size(); i++) {
      if (i > 0) {
        writer.keyword(",");
      }
      FunctionArg functionArg = functionArgList.get(i);
      String argName = functionArg.getName();
      CompleteType argType = CompleteType.deserialize(functionArg.getRawDataType().toByteArray());
      writer.identifier(argName);
      completeTypeToString(writer, argType);
    }
    return writer.toString();
  }

  private static String returnTypeToString(CompleteType type) {
    SqlWriter writer = getSqlWriter();
    if (!type.isScalar()) {
      if (type.isStruct()) {
        fieldListToString(writer, type.getChildren());
      } else {
        throw new IllegalStateException(
            String.format("Invalid tabular function return type: %s", type.getSqlTypeName()));
      }
    } else {
      writer.keyword(type.getSqlTypeName());
    }
    return writer.toString();
  }

  private static String completeTypeToString(SqlWriter writer, CompleteType type) {
    if (type.isStruct()) {
      structTypeToString(writer, type);
    } else if (type.isList()) {
      listTypeToString(writer, type);
    } else if (type.isMap()) {
      mapTypeToString(writer, type);
    } else {
      // Dremio currently only supports complex types: STRUCT, LIST, MAP.
      // https://docs.dremio.com/current/reference/sql/data-types/#semi-structured-data-types
      writer.keyword(type.getSqlTypeName());
    }
    return writer.toString();
  }

  private static void fieldListToString(SqlWriter writer, List<Field> fields) {
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        writer.keyword(",");
      }
      writer.identifier(fields.get(i).getName());
      completeTypeToString(writer, CompleteType.fromField(fields.get(i)));
    }
  }

  private static void structTypeToString(SqlWriter writer, CompleteType type) {
    writer.keyword("ROW");
    writer.keyword("(");
    fieldListToString(writer, type.getChildren());
    writer.keyword(")");
  }

  private static void listTypeToString(SqlWriter writer, CompleteType type) {
    writer.keyword("ARRAY");
    writer.keyword("(");
    completeTypeToString(writer, type.getOnlyChildType());
    writer.keyword(")");
  }

  private static void mapTypeToString(SqlWriter writer, CompleteType type) {
    if (type.getChildren().size() != 1 || type.getOnlyChild().getChildren().size() != 2) {
      throw new IllegalStateException(
          String.format(
              "Invalid map type. Entry has %s fields while expecting 2, i.e. (key, value).",
              type.getOnlyChild().getChildren().size()));
    }
    writer.keyword("MAP");
    writer.keyword("<");
    List<Field> fields = type.getOnlyChild().getChildren();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        writer.keyword(",");
      }
      completeTypeToString(writer, CompleteType.fromField(fields.get(i)));
    }
    writer.keyword(">");
  }

  private static SqlWriter getSqlWriter() {
    StringBuilder buf = new StringBuilder();
    SqlWriterConfig config =
        SqlPrettyWriter.config().withDialect(DREMIO_DIALECT).withQuoteAllIdentifiers(false);
    return new SqlPrettyWriter(config, buf);
  }
}
