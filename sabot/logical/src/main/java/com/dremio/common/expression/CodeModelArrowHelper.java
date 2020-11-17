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

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.ObjectHolder;
import org.apache.arrow.vector.holders.SmallIntHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.TinyIntHolder;
import org.apache.arrow.vector.holders.UInt1Holder;
import org.apache.arrow.vector.holders.UInt2Holder;
import org.apache.arrow.vector.holders.UInt4Holder;
import org.apache.arrow.vector.holders.UInt8Holder;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.common.expression.CompleteType;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JType;

public class CodeModelArrowHelper {
  public static JClass getHolderType(CompleteType type, final JCodeModel model) {
    return model.ref(type.getHolderClass());
  }

  public static JType getHolderType(JCodeModel model, com.dremio.common.types.TypeProtos.MinorType type,
      com.dremio.common.types.TypeProtos.DataMode mode) {
    outside: switch (type) {
    case UNION:
      return model._ref(UnionHolder.class);
    case STRUCT:
    case LIST:
      return model._ref(ComplexHolder.class);

    case TINYINT:
      switch (mode) {
      case REQUIRED:
        return model._ref(TinyIntHolder.class);
      case OPTIONAL:
        return model._ref(NullableTinyIntHolder.class);
      default:
        break outside;
      }
    case UINT1:
      switch (mode) {
      case REQUIRED:
        return model._ref(UInt1Holder.class);
      case OPTIONAL:
        return model._ref(NullableUInt1Holder.class);
      default:
        break outside;
      }
    case UINT2:
      switch (mode) {
      case REQUIRED:
        return model._ref(UInt2Holder.class);
      case OPTIONAL:
        return model._ref(NullableUInt2Holder.class);
      default:
        break outside;
      }
    case SMALLINT:
      switch (mode) {
      case REQUIRED:
        return model._ref(SmallIntHolder.class);
      case OPTIONAL:
        return model._ref(NullableSmallIntHolder.class);
      default:
        break outside;
      }
    case INT:
      switch (mode) {
      case REQUIRED:
        return model._ref(IntHolder.class);
      case OPTIONAL:
        return model._ref(NullableIntHolder.class);
      default:
        break outside;
      }
    case UINT4:
      switch (mode) {
      case REQUIRED:
        return model._ref(UInt4Holder.class);
      case OPTIONAL:
        return model._ref(NullableUInt4Holder.class);
      default:
        break outside;
      }
    case FLOAT4:
      switch (mode) {
      case REQUIRED:
        return model._ref(Float4Holder.class);
      case OPTIONAL:
        return model._ref(NullableFloat4Holder.class);
      default:
        break outside;
      }
    case INTERVALYEAR:
      switch (mode) {
      case REQUIRED:
        return model._ref(IntervalYearHolder.class);
      case OPTIONAL:
        return model._ref(NullableIntervalYearHolder.class);
      default:
        break outside;
      }
    case TIME:
      switch (mode) {
      case REQUIRED:
        return model._ref(TimeMilliHolder.class);
      case OPTIONAL:
        return model._ref(NullableTimeMilliHolder.class);
      default:
        break outside;
      }
    case BIGINT:
      switch (mode) {
      case REQUIRED:
        return model._ref(BigIntHolder.class);
      case OPTIONAL:
        return model._ref(NullableBigIntHolder.class);
      default:
        break outside;
      }
    case UINT8:
      switch (mode) {
      case REQUIRED:
        return model._ref(UInt8Holder.class);
      case OPTIONAL:
        return model._ref(NullableUInt8Holder.class);
      default:
        break outside;
      }
    case FLOAT8:
      switch (mode) {
      case REQUIRED:
        return model._ref(Float8Holder.class);
      case OPTIONAL:
        return model._ref(NullableFloat8Holder.class);
      default:
        break outside;
      }
    case DATE:
      switch (mode) {
      case REQUIRED:
        return model._ref(DateMilliHolder.class);
      case OPTIONAL:
        return model._ref(NullableDateMilliHolder.class);
      default:
        break outside;
      }
    case TIMESTAMP:
      switch (mode) {
      case REQUIRED:
        return model._ref(TimeStampMilliHolder.class);
      case OPTIONAL:
        return model._ref(NullableTimeStampMilliHolder.class);
      default:
        break outside;
      }
    case INTERVALDAY:
      switch (mode) {
      case REQUIRED:
        return model._ref(IntervalDayHolder.class);
      case OPTIONAL:
        return model._ref(NullableIntervalDayHolder.class);
      default:
        break outside;
      }
    case DECIMAL:
      switch (mode) {
      case REQUIRED:
        return model._ref(DecimalHolder.class);
      case OPTIONAL:
        return model._ref(NullableDecimalHolder.class);
      default:
        break outside;
      }
    case FIXEDSIZEBINARY:
      switch (mode) {
      case REQUIRED:
        return model._ref(FixedSizeBinaryHolder.class);
      case OPTIONAL:
        return model._ref(NullableFixedSizeBinaryHolder.class);
      default:
        break outside;
      }
    case VARBINARY:
      switch (mode) {
      case REQUIRED:
        return model._ref(VarBinaryHolder.class);
      case OPTIONAL:
        return model._ref(NullableVarBinaryHolder.class);
      }
    case VARCHAR:
      switch (mode) {
      case REQUIRED:
        return model._ref(VarCharHolder.class);
      case OPTIONAL:
        return model._ref(NullableVarCharHolder.class);
      }
    case BIT:
      switch (mode) {
      case REQUIRED:
        return model._ref(BitHolder.class);
      case OPTIONAL:
        return model._ref(NullableBitHolder.class);
      default:
        break outside;
      }
    case GENERIC_OBJECT: {
      return model._ref(ObjectHolder.class);
    }
    default:
      break;
    }
    throw new UnsupportedOperationException(String.format("Unable to get holder type for minor type [%s]",
        com.dremio.common.util.MajorTypeHelper.getArrowMinorType(type)));
  }
}
