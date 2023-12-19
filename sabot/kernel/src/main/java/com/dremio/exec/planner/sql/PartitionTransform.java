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
package com.dremio.exec.planner.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.parser.SqlPartitionTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Table-format independent representation of a partition transform.
 */
public class PartitionTransform {

  private static final Set<SqlTypeName> TIMESTAMP =
    ImmutableSet.of(SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  private static final Set<SqlTypeName> DATE_OR_TIMESTAMP =
    ImmutableSet.of(SqlTypeName.DATE, SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  private static final Set<SqlTypeName> HASHABLE = ImmutableSet.<SqlTypeName>builder()
    .addAll(SqlTypeName.EXACT_TYPES)
    .addAll(SqlTypeName.STRING_TYPES)
    .addAll(SqlTypeName.DATETIME_TYPES)
    .build();
  private static final Set<SqlTypeName> TRUNCATABLE = ImmutableSet.<SqlTypeName>builder()
    .addAll(SqlTypeName.EXACT_TYPES)
    .addAll(SqlTypeName.CHAR_TYPES)
    .build();
  private static final Set<SqlTypeName> ANY = ImmutableSet.copyOf(SqlTypeName.ALL_TYPES);

  /**
   * Transform types, along with type signature of any arguments they accept.
   */
  public enum Type {
    IDENTITY("identity", ANY),
    YEAR("year", DATE_OR_TIMESTAMP),
    MONTH("month", DATE_OR_TIMESTAMP),
    DAY("day", DATE_OR_TIMESTAMP),
    HOUR("hour", TIMESTAMP),
    BUCKET("bucket", HASHABLE, Integer.class),
    TRUNCATE("truncate", TRUNCATABLE, Integer.class);

    private final String name;
    private final List<Class<?>> argTypes;
    private final Set<SqlTypeName> allowedColumnTypes;

    Type(String name, Set<SqlTypeName> allowedColumnTypes, Class<?>... argTypes) {
      this.name = name;
      this.allowedColumnTypes = allowedColumnTypes;
      this.argTypes = Arrays.asList(argTypes);
    }

    public String getName() {
      return name;
    }

    public Set<SqlTypeName> getAllowedColumnTypes() {
      return allowedColumnTypes;
    }

    public List<Class<?>> getArgTypes() {
      return argTypes;
    }

    public static Type lookup(String name) {
      try {
        return Type.valueOf(name.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException ex) {
        return null;
      }
    }
  }

  private final String columnName;
  private final Type type;
  private final List<Object> arguments;

  public PartitionTransform(String columnName) {
    this(columnName, Type.IDENTITY, ImmutableList.of());
  }

  public PartitionTransform(String columnName, Type type) {
    this(columnName, type, ImmutableList.of());
  }

  public PartitionTransform(String columnName, Type type, List<Object> arguments) {
    this.columnName = Preconditions.checkNotNull(columnName);
    this.type = Preconditions.checkNotNull(type);
    this.arguments = validateAndConvertArguments(type, arguments);
  }

  public static PartitionTransform withColumnName(String columnName, PartitionTransform copyFrom){
    return new PartitionTransform(columnName, copyFrom.getType(), copyFrom.arguments);
  }

  public String getColumnName() {
    return columnName;
  }

  public Type getType() {
    return type;
  }

  public <T> T getArgumentValue(int index, Class<T> clazz) {
    Preconditions.checkArgument(index >= 0 && index < arguments.size() && arguments.get(index) != null &&
      type.getArgTypes().get(index) == clazz);
    return clazz.cast(arguments.get(index));
  }

  public void validateWithSchema(RelDataType rowType) {
    RelDataTypeField field = rowType.getField(columnName, false, false);
    if (field != null) {
      if (!type.getAllowedColumnTypes().contains(field.getType().getSqlTypeName())) {
        throw UserException.validationError()
          .message(
            "Invalid column type for partition transform '%s' on column '%s'.  Allowed types are %s.",
            type.getName(),
            columnName,
            type.getAllowedColumnTypes().stream()
              .map(SqlTypeName::getName)
              .collect(Collectors.joining(", ")))
          .buildSilently();
      }
    } else {
      throw UserException.validationError()
        .message("Invalid column name '%s' for partition transform '%s'.", columnName, type.getName())
        .buildSilently();
    }
  }

  public static void validateWithSchema(List<PartitionTransform> partitionTransforms, RelDataType rowType) {
    for (PartitionTransform partitionTransform : partitionTransforms) {
      partitionTransform.validateWithSchema(rowType);
    }
  }

  private static List<Object> validateAndConvertArguments(Type type, List<Object> arguments) {
    if (arguments == null || arguments.size() != type.getArgTypes().size()) {
      throw UserException.validationError().message("Invalid arguments for partition transform '%s'", type.getName())
        .buildSilently();
    }

    ImmutableList.Builder<Object> convertedArguments = ImmutableList.builder();
    for (int i = 0; i < arguments.size(); i++) {
      convertedArguments.add(tryConvertArgumentValue(type, arguments.get(i), type.getArgTypes().get(i)));
    }

    return convertedArguments.build();
  }

  private static Object tryConvertArgumentValue(Type type, Object value, Class<?> argumentType) {
    //for now this code is only used for BUCKET and TRUNCATE transforms
    //the rest of the supported transforms do not take additional arguments,
    //so the arguments.size() would be zero and this function is never called
    if (argumentType == Integer.class) {
      if (value instanceof Number) {
        return ((Number) value).intValue();
      } else if(value instanceof String){
        try {
          return Integer.parseInt((String) value);
        }catch(final NumberFormatException e){
          throw UserException.validationError().message("Invalid argument '%s' for partition transform '%s'", value, type.getName())
            .buildSilently();
        }
      }
    }

    throw UserException.validationError().message("Invalid arguments for partition transform '%s'", type.getName())
      .buildSilently();
  }

  public static PartitionTransform from(SqlPartitionTransform sqlPartitionTransform) {
    String transformName = sqlPartitionTransform.getTransformName().toString();
    Type type = Type.lookup(transformName);
    if (type == null) {
      throw UserException.validationError()
        .message("Unknown partition transform '%s'", transformName).buildSilently();
    }

    List<Object> args = sqlPartitionTransform.getTransformArguments().stream()
      .map(SqlLiteral::getValue).collect(Collectors.toList());
    return new PartitionTransform(sqlPartitionTransform.getColumnName().toString(), type, args);
  }

  @Override
  public String toString() {
    StringBuilder transformation = new StringBuilder();
    transformation.append(type);
    transformation.append("(");
    switch (type) {
      case BUCKET:
      case TRUNCATE:
        transformation.append(arguments.stream().map(String::valueOf).collect(Collectors.joining(",")));
        transformation.append(",");
        break;

      case IDENTITY:
      case HOUR:
      case DAY:
      case MONTH:
      case YEAR:
        break;

      default:
        throw UserException.validationError()
                .message("Unknown partition transform '%s'", type).buildSilently();
    }
    transformation.append(columnName);
    transformation.append(")");
    return transformation.toString();
  }

  public boolean isIdentity(){
    return Type.IDENTITY.equals(getType());
  }
}
