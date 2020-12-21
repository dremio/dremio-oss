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

public class SqlDisplaySizeVisitor implements ArrowTypeVisitor<Integer>{

  @Override
  public Integer visit(Null paramNull) {
    return 0;
  }

  @Override
  public Integer visit(Struct paramStruct) {
    return 0;
  }

  @Override
  public Integer visit(List paramList) {
    return 0;
  }

  @Override
  public Integer visit(Union paramUnion) {
    return 0;
  }

  @Override
  public Integer visit(Int paramInt) {
    switch(paramInt.getBitWidth()){
    case 8:         return 4; // sign + 3 digit
    case 16:        return 6; // sign + 5 digits
    case 32:        return 11; // sign + 10 digits
    case 64:        return 20; // sign + 19 digits
    }
    throw new IllegalStateException("Unknown int width " + paramInt.getBitWidth());
  }

  @Override
  public Integer visit(FloatingPoint paramFloatingPoint) {
    switch(paramFloatingPoint.getPrecision()){
    case SINGLE: return 14; // sign + 7 digits + decimal point + E + 2 digits
    case DOUBLE: return 24; // sign + 15 digits + decimal point + E + 3 digits
    default:
      throw new IllegalStateException("unable to report width for floating point of width " + paramFloatingPoint.getPrecision());
    }
  }

  @Override
  public Integer visit(Utf8 paramUtf8) {
    return 65536;
  }

  @Override
  public Integer visit(Binary paramBinary) {
    return 65536;
  }

  @Override
  public Integer visit(Bool paramBool) {
    return 1; // 1 digit
  }

  @Override
  public Integer visit(Decimal paramDecimal) {
    return 2 + paramDecimal.getPrecision();
  }

  @Override
  public Integer visit(Date paramDate) {
    return 10; // hh-mm-ss
  }

  @Override
  public Integer visit(Time paramTime) {
    return 8; // hh-mm-ss
  }

  @Override
  public Integer visit(Timestamp paramTimestamp) {
    return 23; // yyyy-mm-ddThh:mm:ss
  }

  @Override
  public Integer visit(Interval paramInterval) {
    switch(paramInterval.getUnit()){
    case DAY_TIME: return 0;
    case YEAR_MONTH: return 0;
    default:
      throw new IllegalStateException("unable to determine width for interval with unit " + paramInterval.getUnit());
    }
  }

  @Override
  public Integer visit(FixedSizeList paramList) {
    return 0;
  }

  @Override
  public Integer visit(FixedSizeBinary paramList) {
    return 0;
  }

  @Override
  public Integer visit(LargeBinary paramLargeBinary) {
    throw new UnsupportedOperationException("Dremio does not support LargeBinary.");
  }

  @Override
  public Integer visit(LargeList paramLargeList) {
    throw new UnsupportedOperationException("Dremio does not support LargeList");
  }

  @Override
  public Integer visit(LargeUtf8 paramLargeUtf8) {
    throw new UnsupportedOperationException("Dremio does not support LargeUtf8.");
  }

  @Override
  public Integer visit(ArrowType.Duration paramDuration) {
    throw new UnsupportedOperationException("Dremio does not support duration.");
  }

  @Override
  public Integer visit(ArrowType.Map paramDuration) {
    throw new UnsupportedOperationException("Dremio does not support map.");
  }

}
