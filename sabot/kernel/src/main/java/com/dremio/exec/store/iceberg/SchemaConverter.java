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
package com.dremio.exec.store.iceberg;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeList;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.NestedType;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.Lists;

/**
 * Converter for iceberg schema to BatchSchema, and vice-versa.
 */
public class SchemaConverter {

  public SchemaConverter() {
  }

  public BatchSchema fromIceberg(org.apache.iceberg.Schema icebergSchema) {
    return new BatchSchema(icebergSchema
      .columns()
      .stream()
      .map(SchemaConverter::fromIcebergColumn)
      .filter(Objects::nonNull)
      .collect(Collectors.toList()));
  }

  public static Field fromIcebergColumn(NestedField field) {
    try {
      CompleteType fieldType = fromIcebergType(field.type());
      return fieldType == null ? null : fieldType.toField(field.name());
    } catch (Exception e) {
      throw UserException.unsupportedError(e)
        .message("conversion from iceberg type to arrow type failed for field " + field.name())
        .buildSilently();
    }
  }

  public static CompleteType fromIcebergType(Type type) {
    if (type.isPrimitiveType()) {
      return fromIcebergPrimitiveType(type.asPrimitiveType());
    } else {
      NestedType nestedType = type.asNestedType();
      if (nestedType.isListType()) {
        ListType listType = (ListType)nestedType;
        NestedField elementField = listType.fields().get(0);
        CompleteType elementType = fromIcebergType(elementField.type());
        return (elementType == null) ? null : elementType.asList();
      } else if (nestedType.isStructType()) {
        StructType structType = (StructType)nestedType;
        List<Types.NestedField> structFields = structType.fields();
        List<Field> innerFields = Lists.newArrayList();
        for (Types.NestedField nestedField : structFields) {
          Field field = fromIcebergColumn(nestedField);
          if (field == null) {
            return null;
          }
          innerFields.add(field);
        }
        return CompleteType.struct(innerFields);
      } else {
        // drop map type and all other unknown iceberg column types
        return null;
      }
    }
  }

  public static CompleteType fromIcebergPrimitiveType(PrimitiveType type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return CompleteType.BIT;
      case INTEGER:
        return CompleteType.INT;
      case LONG:
        return CompleteType.BIGINT;
      case FLOAT:
        return CompleteType.FLOAT;
      case DOUBLE:
        return CompleteType.DOUBLE;
      case STRING:
        return CompleteType.VARCHAR;
      case BINARY:
        return CompleteType.VARBINARY;
      case UUID:
        return new CompleteType(new FixedSizeBinary(16));
      case DATE:
        return CompleteType.DATE;
      case TIME:
        // TODO: When we support Time and Timestamp MICROS, this needs to be changed  to use
        // the existing schema definition for older tables, and to use MICROS for newer tables
        return CompleteType.TIME;
      case TIMESTAMP:
        {
          if (((TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
            return CompleteType.TIMESTAMP;
          } else {
            throw new UnsupportedOperationException("iceberg timestamp type without zone not supported");
          }
        }
      case FIXED:
        return new CompleteType(new FixedSizeBinary(((FixedType)type).length()));
      case DECIMAL:
        DecimalType decimalType = (DecimalType)type;
        return new CompleteType(new Decimal(decimalType.precision(), decimalType.scale()));
      default:
        throw new UnsupportedOperationException("Unsupported iceberg type : " + type);
    }
  }

  public static class NextIDImpl implements TypeUtil.NextID {
    private int id = 0;
    public int get() {
      int curid = id;
      id++;
      return curid;
    }
  }

  public org.apache.iceberg.Schema toIceberg(BatchSchema schema) {
    NextIDImpl id = new NextIDImpl();
    return toIceberg(schema, id);
  }

  public org.apache.iceberg.Schema toIceberg(BatchSchema schema, TypeUtil.NextID id) {
    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(schema
      .getFields()
      .stream()
      .filter(x -> !x.getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD))
      .map(x -> toIcebergColumn(x, id))
      .collect(Collectors.toList()));

    return TypeUtil.assignIncreasingFreshIds(icebergSchema);
  }

  public static NestedField changeIcebergColumn(Field field, NestedField icebergField) {
    try {
      return NestedField.optional(icebergField.fieldId(), field.getName(),
        icebergField.type().isPrimitiveType() ? toIcebergType(CompleteType.fromField(field), new NextIDImpl()) : icebergField.type());
    } catch (Exception e) {
      throw UserException.unsupportedError(e)
        .message("conversion from arrow type to iceberg type failed for field " + field.getName())
        .buildSilently();
    }
  }

  public static NestedField toIcebergColumn(Field field, TypeUtil.NextID id) {
    try {
      return NestedField.optional(id.get(), field.getName(), toIcebergType(CompleteType.fromField(field), id));
    } catch (Exception e) {
      throw UserException.unsupportedError(e)
        .message("conversion from arrow type to iceberg type failed for field " + field.getName())
        .buildSilently();
    }
  }

  private static Type toIcebergType(CompleteType completeType, TypeUtil.NextID id) {
    ArrowType arrowType = completeType.getType();
    return arrowType.accept(new ArrowTypeVisitor<Type>() {
      @Override
      public Type visit(Null aNull) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(Struct struct) {
        List<NestedField> children = completeType
          .getChildren()
          .stream()
          .map(x -> toIcebergColumn(x, id))
          .collect(Collectors.toList());
        return StructType.of(children);
      }

      @Override
      public Type visit(ArrowType.List list) {
        NestedField inner = toIcebergColumn(completeType.getOnlyChild(), id);
        return ListType.ofOptional(inner.fieldId(), inner.type());
      }

      @Override
      public Type visit(FixedSizeList fixedSizeList) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(Union union) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(Map map) {
        List<NestedField> children = completeType
          .getChildren()
          .stream()
          .map(x -> toIcebergColumn(x, id))
          .collect(Collectors.toList());
        return MapType.ofOptional(
          children.get(0).fieldId(),
          children.get(1).fieldId(),
          children.get(0).type(),
          children.get(1).type());
      }

      @Override
      public Type visit(Int anInt) {
        return anInt.getBitWidth() == 32 ? IntegerType.get() : LongType.get();
      }

      @Override
      public Type visit(FloatingPoint floatingPoint) {
        switch (floatingPoint.getPrecision()) {
          case SINGLE:
            return FloatType.get();
          case DOUBLE:
            return DoubleType.get();
          default:
            throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
        }
      }

      @Override
      public Type visit(Utf8 utf8) {
        return StringType.get();
      }

      @Override
      public Type visit(Binary binary) {
        return BinaryType.get();
      }

      @Override
      public Type visit(FixedSizeBinary fixedSizeBinary) {
        return FixedType.ofLength(fixedSizeBinary.getByteWidth());
      }

      @Override
      public Type visit(LargeBinary largeBinary) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(LargeList largeList) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(LargeUtf8 largeUtf8) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(Bool bool) {
        return BooleanType.get();
      }

      @Override
      public Type visit(Decimal decimal) {
        return DecimalType.of(decimal.getPrecision(), decimal.getScale());
      }

      @Override
      public Type visit(Date date) {
        return DateType.get();
      }

      @Override
      public Type visit(Time time) {
        return TimeType.get();
      }

      @Override
      public Type visit(Timestamp timestamp) {
        return TimestampType.withZone();
      }

      @Override
      public Type visit(Interval interval) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }

      @Override
      public Type visit(Duration duration) {
        throw new UnsupportedOperationException("Unsupported arrow type : " + arrowType);
      }
    });
  }

  public static Schema getChildSchemaForStruct(Schema schema, String structName) {
    if (schema == null) {
      return null;
    }

    NestedField structField = schema.findField(structName);
    if (!structField.type().isStructType()) {
      return null;
    }

    return new Schema(structField.type().asStructType().fields());
  }

  public static Schema getChildSchemaForList(Schema schema, String listName) {
    if (schema == null) {
      return null;
    }

    NestedField listField = schema.findField(listName);
    if (!listField.type().isListType()) {
      return null;

    }

    return new Schema(listField.type().asListType().fields().get(0));
  }
}
