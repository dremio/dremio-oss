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
package com.dremio.common.expression;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeList;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Describes Field, CompleteType and ArrowType in human readable form.
 */
public class Describer {

  private static TypeDescriber INSTANCE = new TypeDescriber();

  public static String describe(ArrowType type){
    return type.accept(INSTANCE);
  }

  public static String describe(CompleteType type){
    if(type == CompleteType.OBJECT){
      return "object";
    }
    if(type == CompleteType.NULL){
      return "null";
    }
    if(type == CompleteType.LATE){
      return "late";
    }
    return describe(type.toField(""), false);
  }

  public static String describe(Iterable<Field> fields){
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for(Field f : fields){
      if(!first){
        sb.append(", ");
      }
      sb.append(describe(f));
      first = false;
    }
    return sb.toString();
  }

  public static String describeInternal(ArrowType type){
    Types.MinorType mtype = Types.getMinorTypeForArrowType(type);
    return mtype.name().toLowerCase();
  }

  public static String describeWithLineBreaks(Iterable<Field> fields){
    StringBuilder sb = new StringBuilder();
    for(Field f : fields){
      sb.append(describe(f));
      sb.append("\n");
    }
    return sb.toString();
  }


  public static String describe(Field field){
    return field.getType().accept(new FieldDescriber(field, true));
  }

  private static String describe(Field field, boolean includeName){
    return field.getType().accept(new FieldDescriber(field, includeName));
  }

  public static final class FieldDescriber extends AbstractArrowTypeVisitor<String> {

    private final Field field;
    private final boolean includeName;

    public FieldDescriber(Field field, boolean includeName){
      this.field = field;
      this.includeName = includeName;
    }

    @Override
    public String visit(Struct type) {
      StringBuilder sb = new StringBuilder();
      if(includeName){
        sb.append(field.getName());
        sb.append("::");
      }

      sb.append("struct<");
      boolean first = true;
      for (Field f : field.getChildren()) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(describe(f, true));
      }
      sb.append(">");
      return sb.toString();
    }

    @Override
    public String visit(List type) {
      StringBuilder sb = new StringBuilder();
      if(includeName){
        sb.append(field.getName());
        sb.append("::");
      }

      sb.append("list<");
      sb.append(describe(field.getChildren().get(0), false));
      sb.append(">");

      return sb.toString();
    }

    @Override
    public String visit(Union type) {
      StringBuilder sb = new StringBuilder();
      if(includeName){
        sb.append(field.getName());
        sb.append("::");
      }

      sb.append("union<");
      boolean first = true;
      for(Field f : field.getChildren()){
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(describe(f, false));
      }
      sb.append(">");
      return sb.toString();
    }

    @Override
    protected String visitGeneric(ArrowType type) {
      String typeStr = describe(type);
      if(!includeName) {
        return typeStr;
      }

      return field.getName() + "::" + typeStr;
    }

  }

  private static class TypeDescriber implements ArrowTypeVisitor<String> {


    private TypeDescriber(){}

    @Override
    public String visit(Null type) {
      return "null";
    }

    @Override
    public String visit(Struct type) {
      return "struct";
    }

    @Override
    public String visit(List type) {
      return "list";
    }

    @Override
    public String visit(Union type) {
      return "union";
    }

    @Override
    public String visit(Int type) {
      return (type.getIsSigned() ? "" : "u") + "int" + type.getBitWidth();
    }

    @Override
    public String visit(FloatingPoint type) {
      return type.getPrecision() == FloatingPointPrecision.SINGLE ? "float" : "double";
    }

    @Override
    public String visit(Utf8 type) {
      return "varchar";
    }

    @Override
    public String visit(Binary type) {
      return "varbinary";
    }

    @Override
    public String visit(Bool type) {
      return "boolean";
    }

    @Override
    public String visit(Decimal type) {
      return String.format("decimal(%d,%d)", type.getPrecision(), type.getScale());
    }

    @Override
    public String visit(Date type) {
      String name = "date";
      DateUnit unit = type.getUnit();
      if (unit == DateUnit.MILLISECOND) {
        return name;
      }
      return String.format("%s(%s)", name, unit);
    }

    @Override
    public String visit(Time type) {
      String name = "time";
      TimeUnit unit = type.getUnit();
      if (unit == TimeUnit.MILLISECOND) {
        return name;
      }
      return String.format("%s(%s)", name, unit);
    }

    @Override
    public String visit(Timestamp type) {
      String name = "timestamp";
      String timezone = type.getTimezone();
      TimeUnit unit = type.getUnit();
      if (timezone == null && unit == TimeUnit.MILLISECOND) {
        return name;
      }
      return String.format("%s(%s,%s)", name, timezone == null ? "?" : timezone, unit.name());
    }

    @Override
    public String visit(Interval type) {
      return type.getUnit() == IntervalUnit.DAY_TIME ? "interval_day" : "interval_year";
    }

    @Override
    public String visit(FixedSizeList type) {
      return String.format("list(%d)", type.getListSize());
    }

    @Override
    public String visit(FixedSizeBinary type) {
      return String.format("binary(%d)", type.getByteWidth());
    }

    @Override
    public String visit(LargeBinary paramLargeBinary) {
      throw new UnsupportedOperationException("Dremio does not support LargeBinary yet");
    }

    @Override
    public String visit(LargeList paramLargeList) {
      throw new UnsupportedOperationException("Dremio does not support LargeList yet");
    }

    @Override
    public String visit(LargeUtf8 paramLargeUtf8) {
      throw new UnsupportedOperationException("Dremio does not support LargeUtf8 yet");
    }

    @Override
    public String visit(ArrowType.Duration type) {
      throw new UnsupportedOperationException("Dremio does not support duration yet.");
    }

    @Override
    public String visit(ArrowType.Map type) {
      throw new UnsupportedOperationException("Dremio does not support map yet.");
    }

  }

}
