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
package com.dremio.exec.planner.serializer;

import java.nio.charset.Charset;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlCollation.Coercibility;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;

import com.dremio.plan.serialization.PRelDataType;
import com.dremio.plan.serialization.PRelDataTypeField;
import com.dremio.plan.serialization.PSqlCollation;
import com.dremio.plan.serialization.PSqlTypeName;
import com.dremio.plan.serialization.PStructKind;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;

/**
 * Conversts PSqlTypeName protos to/from org.apache.calcite.sql.type.SqlTypeName
 */
public final class TypeSerde {

  private static final ImmutableBiMap<PSqlTypeName, SqlTypeName> TYPE_NAMES;

  private final RelDataTypeFactory factory;

  public TypeSerde(RelDataTypeFactory factory) {
    super();
    this.factory = factory;
  }

  public PRelDataType toProto(RelDataType type) {
    // Related to Abstract Type
    PSqlTypeName name = toProto(type.getSqlTypeName());
    final PRelDataType.Builder builder = PRelDataType
      .newBuilder()
      .setNullable(type.isNullable())
      .setTypeName(name);

    // Related to Basic Type
    if(type.getSqlTypeName().allowsScale()) {
      builder.setScale(type.getScale());
    }

    if(type.getSqlTypeName().allowsPrec()) {
      builder.setPrecision(type.getPrecision());
    }

    if(type.getCollation() != null) {
      builder.setCollation(toProto(type.getCollation()));
    }

    if (type.getCharset() != null) {
      builder.setCharset(type.getCharset().name());
    }

    // Related To Map Type
    if(type.getSqlTypeName().equals(SqlTypeName.MAP)) {
      builder.setKeyType(toProto(type.getKeyType()));
      builder.setValueType(toProto(type.getValueType()));
    }

    // Related to Struct Type
    if(type.getSqlTypeName() == SqlTypeName.STRUCTURED || type.getSqlTypeName() == SqlTypeName.ROW) {
      builder
        .setStructKind(toProto(type.getStructKind()))
        .addAllChildren(
          type.getFieldList()
            .stream()
            .map(this::toProto)
            .collect(Collectors.toList())
        );
    }

    // Related to Array Type
    if(type.getSqlTypeName() == SqlTypeName.ARRAY) {
      builder.setComponentType(toProto(type.getComponentType()));
    }

    return builder.build();
  }

  public RelDataType fromProto(PRelDataType type) {
    RelDataType baseType = fromProtoBase(type);
    if(!type.getNullable()) {
      return baseType;
    }

    return factory.createTypeWithNullability(baseType, type.getNullable());
  }

  public PRelDataTypeField toProto(RelDataTypeField field) {
    return PRelDataTypeField.newBuilder()
        .setName(field.getName())
        .setIndex(field.getIndex())
        .setType(toProto(field.getType()))
        .build();
  }

  public PSqlCollation toProto(SqlCollation c) {
    return PSqlCollation.newBuilder()
        .setName(c.getCollationName())
        .setCoercibility(toProto(c.getCoercibility()))
        .build();
  }

  public SqlCollation fromProto(PSqlCollation c) {
    final String  collationName = c.getName();
    final Coercibility coercibility = fromProto(c.getCoercibility());
    if (collationName.equals(SqlCollation.IMPLICIT.getCollationName())
        && coercibility == Coercibility.IMPLICIT) {
      return SqlCollation.IMPLICIT;
    } else if (collationName.equals(SqlCollation.COERCIBLE.getCollationName())
        && coercibility == Coercibility.COERCIBLE) {
      return SqlCollation.COERCIBLE;
    }
    return new SqlCollation(collationName, coercibility);
  }

  public RelDataTypeFactory getFactory() {
    return this.factory;
  }

  private PSqlCollation.Coercibility toProto(Coercibility c) {
    switch(c) {
    case COERCIBLE:
      return PSqlCollation.Coercibility.COERCIBLE;
    case EXPLICIT:
      return PSqlCollation.Coercibility.EXPLICIT;
    case IMPLICIT:
      return PSqlCollation.Coercibility.IMPLICIT;
    case NONE:
      return PSqlCollation.Coercibility.NONE;
    default:
      throw new IllegalStateException("Unknown coercibility: " + c.name());
    }
  }

  private Coercibility fromProto(PSqlCollation.Coercibility c) {
    switch(c) {
    case COERCIBLE:
      return Coercibility.COERCIBLE;
    case EXPLICIT:
      return Coercibility.EXPLICIT;
    case IMPLICIT:
      return Coercibility.IMPLICIT;
    case NONE:
      return Coercibility.NONE;
    case UNRECOGNIZED:
    default:
      throw new IllegalStateException("Unknown coercibility: " + c.name());
    }
  }

  private RelDataType fromProtoBase(PRelDataType type) {
    final SqlTypeName name = fromProto(type.getTypeName());
    final int precision = (name.allowsPrec() && type.getPrecision() != SqlTypeName.DEFAULT_INTERVAL_START_PRECISION) ?
      type.getPrecision() : RelDataType.PRECISION_NOT_SPECIFIED;

    switch(type.getTypeName()) {

    // scalar types.
    case ANY:
    case BIGINT:
    case BINARY:
    case BOOLEAN:
    case DATE:
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
    case TIME:
    case TIMESTAMP:
    case INTEGER:
    case VARBINARY:
    {
      if(name.allowsPrec()) {
        if(name.allowsScale()) {
          return factory.createSqlType(name, type.getPrecision(), type.getScale());
        }

        return factory.createSqlType(name, type.getPrecision());
      }

      return factory.createSqlType(name);
    }


    // char types.
    case CHAR:
    case VARCHAR:
      final RelDataType newType = name.allowsPrec() ? factory.createSqlType(name, type.getPrecision()) : factory.createSqlType(name);
      final Charset charset = StringUtils.isEmpty(type.getCharset()) ? factory.getDefaultCharset() : Charset.forName(type.getCharset());
      final SqlCollation sqlCollation = fromProto(type.getCollation());

      return factory.createTypeWithCharsetAndCollation(newType, charset, sqlCollation);

    // interval types
    case INTERVAL_DAY:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.DAY, precision, TimeUnit.DAY, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_DAY_HOUR:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.DAY, precision, TimeUnit.HOUR, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_DAY_MINUTE:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.DAY, precision, TimeUnit.MINUTE, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_DAY_SECOND:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.DAY, precision, TimeUnit.SECOND, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_HOUR:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.HOUR, precision, TimeUnit.HOUR, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_HOUR_MINUTE:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.HOUR, precision, TimeUnit.MINUTE, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_HOUR_SECOND:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.HOUR, precision, TimeUnit.SECOND, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_MINUTE:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.MINUTE, precision, TimeUnit.MINUTE, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_MINUTE_SECOND:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.MINUTE, precision, TimeUnit.SECOND, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_MONTH:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.MONTH, precision, TimeUnit.MONTH, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_SECOND:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.SECOND, precision, TimeUnit.SECOND, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_YEAR:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.YEAR, precision, TimeUnit.YEAR, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));
    case INTERVAL_YEAR_MONTH:
      return factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.YEAR, precision, TimeUnit.MONTH, RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO));


    // map and struct types.
    case MAP:
      return factory.createMapType(fromProto(type.getKeyType()), fromProto(type.getValueType()));

    case ROW:
    case STRUCTURED:
      return factory.createStructType(
          fromProto(type.getStructKind()),
          type.getChildrenList().stream().map(c -> fromProto(c.getType())).collect(Collectors.toList()),
          type.getChildrenList().stream().map(c -> c.getName()).collect(Collectors.toList())
          );
    case SYMBOL:
      return factory.createSqlType(SqlTypeName.SYMBOL);
    case DYNAMIC_STAR:
      return factory.createSqlType(SqlTypeName.DYNAMIC_STAR);

    case ARRAY:
      return factory.createArrayType(fromProto(type.getComponentType()), -1);
    case NULL:
      return factory.createSqlType(SqlTypeName.NULL);
    case COLUMN_LIST:
    case CURSOR:
    case DISTINCT:
    case GEOMETRY:
    case MULTISET:
    case OTHER:
    case REAL:
    case SMALLINT:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TINYINT:
    case UNKNOWN_SQL_TYPE_NAME:
    case UNRECOGNIZED:
    default:
      break;
    }

    throw new UnsupportedOperationException(String.format("Unhandled serialization type: %s.", type));
  }

  private static StructKind fromProto(PStructKind kind) {
    switch(kind) {
    case FULLY_QUALIFIED:
      return StructKind.FULLY_QUALIFIED;
    case NONE_STRUCT_KIND:
      return StructKind.NONE;
    case PEEK_FIELDS:
      return StructKind.PEEK_FIELDS;
    case PEEK_FIELDS_DEFAULT:
      return StructKind.PEEK_FIELDS_DEFAULT;
    case PEEK_FIELDS_NO_EXPAND:
      return StructKind.PEEK_FIELDS_NO_EXPAND;
    case UNKNOWN_STRUCT_KIND:
    case UNRECOGNIZED:
    default:
      throw new UnsupportedOperationException(String.format("Unhandled struct kind: %s.", kind));
    }
  }

  private static PStructKind toProto(StructKind kind) {
    switch(kind) {
    case FULLY_QUALIFIED:
      return PStructKind.FULLY_QUALIFIED;
    case NONE:
      return PStructKind.NONE_STRUCT_KIND;
    case PEEK_FIELDS:
      return PStructKind.PEEK_FIELDS;
    case PEEK_FIELDS_DEFAULT:
      return PStructKind.PEEK_FIELDS_DEFAULT;
    case PEEK_FIELDS_NO_EXPAND:
      return PStructKind.PEEK_FIELDS_NO_EXPAND;
    default:
      throw new UnsupportedOperationException(String.format("Unhandled struct kind: %s.", kind));
    }
  }

  static {
    ImmutableBiMap.Builder<SqlTypeName, PSqlTypeName> builder = ImmutableBiMap.builder();
    for(SqlTypeName name : SqlTypeName.values()) {
      builder.put(name, PSqlTypeName.valueOf(name.name()));
    }
    TYPE_NAMES = builder.build().inverse();
  }

  public static SqlTypeName fromProto(PSqlTypeName name) {
    return Preconditions.checkNotNull(TYPE_NAMES.get(name));
  }

  public static PSqlTypeName toProto(SqlTypeName name) {
    return Preconditions.checkNotNull(TYPE_NAMES.inverse().get(name));
  }
}
