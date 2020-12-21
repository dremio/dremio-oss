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

/***
 * Gets SQL data type name for given Dremio RPC-/protobuf-level data type.
 * returns the canonical keyword sequence for SQL data type (leading keywords in
 * corresponding {@code <data type>}; what
 * {@code INFORMATION_SCHEMA.COLUMNS.TYPE_NAME} would list)
 */
public class SqlTypeNameVisitor implements ArrowTypeVisitor<String> {

  @Override
  public String visit(Null paramNull) {
    return "NULL";
  }

  @Override
  public String visit(Struct paramStruct_) {
    return "MAP";
  }

  @Override
  public String visit(List paramList) {
    return "ARRAY";
  }

  @Override
  public String visit(Union paramUnion) {
    return "UNION";
  }

  @Override
  public String visit(Int paramInt) {
    switch(paramInt.getBitWidth()){
    case 8: return "TINYINT";
    case 16: return "SMALLINT";
    case 32: return "INTEGER";
    case 64: return "BIGINT";
    default:
      throw new IllegalStateException("unable to report sql type for integer of width " + paramInt.getBitWidth());
    }
  }

  @Override
  public String visit(FloatingPoint paramFloatingPoint) {
    switch(paramFloatingPoint.getPrecision()){
    case SINGLE: return "FLOAT";
    case DOUBLE: return "DOUBLE";
    default:
      throw new IllegalStateException("unable to report sql type for floating point of width " + paramFloatingPoint.getPrecision());
    }
  }

  @Override
  public String visit(Utf8 paramUtf8) {
    return "CHARACTER VARYING";
  }

  @Override
  public String visit(Binary paramBinary) {
    return "BINARY VARYING";
  }

  @Override
  public String visit(Bool paramBool) {
    return "BOOLEAN";
  }

  @Override
  public String visit(Decimal paramDecimal) {
    return "DECIMAL";
  }

  @Override
  public String visit(Date paramDate) {
    return "DATE";
  }

  @Override
  public String visit(Time paramTime) {
    return "TIME";
  }

  @Override
  public String visit(Timestamp paramTimestamp) {
    return "TIMESTAMP";
  }

  @Override
  public String visit(Interval paramInterval) {
    switch(paramInterval.getUnit()){
    case DAY_TIME: return "INTERVAL DAY TO SECOND";
    case YEAR_MONTH: return "INTERVAL YEAR TO MONTH";
    default:
      throw new IllegalStateException("unable to determine sql type for interval with unit " + paramInterval.getUnit());
    }
  }

  @Override
  public String visit(FixedSizeList paramList) {
    return "ARRAY";
  }

  @Override
  public String visit(FixedSizeBinary paramList) {
    return "BINARY";
  }

  @Override
  public String visit(LargeBinary paramLargeBinary) {
    throw new UnsupportedOperationException("Dremio does not support LargeBinary");
  }

  @Override
  public String visit(LargeList paramLargeList) {
    throw new UnsupportedOperationException("Dremio does not support LargeList");
  }

  @Override
  public String visit(LargeUtf8 paramLargeUtf8) {
    throw new UnsupportedOperationException("Dremio does not support LargeUtf8");
  }

  @Override
  public String visit(ArrowType.Duration paramDuration) {
    throw new UnsupportedOperationException("Dremio does not support duration.");
  }

  @Override
  public String visit(ArrowType.Map paramDuration) {
    throw new UnsupportedOperationException("Dremio does not support map.");
  }
}
