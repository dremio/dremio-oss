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
package com.dremio.exec.expr;

import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FixedWidthVectorHelper;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.NullVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVectorHelper;
import org.apache.arrow.vector.VariableWidthVectorHelper;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.ZeroVectorHelper;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListVectorHelper;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.NonNullableStructVectorHelper;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.StructVectorHelper;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.UnionVectorHelper;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
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
import org.apache.arrow.vector.holders.SmallIntHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.TinyIntHolder;
import org.apache.arrow.vector.holders.UInt1Holder;
import org.apache.arrow.vector.holders.UInt2Holder;
import org.apache.arrow.vector.holders.UInt4Holder;
import org.apache.arrow.vector.holders.UInt8Holder;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;

import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;

/**
 * generated from TypeHelper.java
 */
public class TypeHelper extends BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  public static <T extends ValueVector> Class<T> getValueVectorClass(Field field) {
    return (Class<T>) getValueVectorClass(getMinorTypeForArrowType(field.getType()));
  }

  public static void load(ValueVector v, SerializedField metadata, ArrowBuf buffer) {
    Optional<ValueVectorHelper> helper = getHelper(v);

    if (!helper.isPresent()) {
      throw new UnsupportedOperationException(String.format("no loader for vector %s", v));
    }

    helper.get().load(metadata, buffer);
  }

  public static void loadFromValidityAndDataBuffers(ValueVector v, SerializedField metadata, ArrowBuf dataBuffer,
      ArrowBuf validityBuffer) {
    if (v instanceof ZeroVector) {
      throw new UnsupportedOperationException(String.format("this loader is not supported for vector %s", v));
    } else if (v instanceof UnionVector) {
      throw new UnsupportedOperationException(String.format("this loader is not supported for vector %s", v));
    } else if (v instanceof ListVector) {
      throw new UnsupportedOperationException(String.format("this loader is not supported for vector %s", v));
    } else if (v instanceof StructVector) {
      throw new UnsupportedOperationException(String.format("this loader is not supported for vector %s", v));
    } else if (v instanceof NonNullableStructVector) {
      throw new UnsupportedOperationException(String.format("this loader is not supported for vector %s", v));
    }

    Optional<ValueVectorHelper> helper = getHelper(v);

    if (!helper.isPresent()) {
      throw new UnsupportedOperationException(String.format("no loader for vector %s", v));
    }

    helper.get().loadFromValidityAndDataBuffers(metadata, dataBuffer, validityBuffer);
  }

  public static void loadData(ValueVector v, SerializedField metadata, ArrowBuf buffer) {
    Optional<ValueVectorHelper> helper = getHelper(v);

    if (!helper.isPresent()) {
      throw new UnsupportedOperationException(String.format("no loader for vector %s", v));
    }

    helper.get().loadData(metadata, buffer);
  }

  public static SerializedField.Builder getMetadataBuilder(ValueVector v) {
    return getHelper(v).map(t -> t.getMetadataBuilder()).orElse(null);
  }

  public static Optional<ValueVectorHelper> getHelper(ValueVector v) {
    return Optional.ofNullable(getHelperNull(v));
  }

  private static ValueVectorHelper getHelperNull(ValueVector v) {
    if (v instanceof ZeroVector) {
      return new ZeroVectorHelper((ZeroVector) v);
    } else if (v instanceof NullVector) {
      return new NullVectorHelper((NullVector) v);
    } else if (v instanceof UnionVector) {
      return new UnionVectorHelper((UnionVector) v);
    } else if (v instanceof ListVector) {
      return new ListVectorHelper((ListVector) v);
    } else if (v instanceof StructVector) {
      return new StructVectorHelper((StructVector) v);
    } else if (v instanceof NonNullableStructVector) {
      return new NonNullableStructVectorHelper((NonNullableStructVector) v);
    } else if (v instanceof BaseFixedWidthVector) {
      return new FixedWidthVectorHelper<BaseFixedWidthVector>((BaseFixedWidthVector) v);
    } else if (v instanceof BaseVariableWidthVector) {
      return new VariableWidthVectorHelper<BaseVariableWidthVector>((BaseVariableWidthVector) v);
    }

    return null;
  }

  public static SerializedField getMetadata(ValueVector v) {
    return getHelper(v).map(t -> t.getMetadata()).orElse(null);
  }

  public static ValueHolder getRequiredHolder(ValueHolder h) {
    if (h instanceof NullableTinyIntHolder) {
      NullableTinyIntHolder holder = (NullableTinyIntHolder) h;
      TinyIntHolder TinyIntHolder = new TinyIntHolder();
      TinyIntHolder.value = holder.value;
      return TinyIntHolder;
    }
    if (h instanceof NullableUInt1Holder) {
      NullableUInt1Holder holder = (NullableUInt1Holder) h;
      UInt1Holder UInt1Holder = new UInt1Holder();
      UInt1Holder.value = holder.value;
      return UInt1Holder;
    }
    if (h instanceof NullableUInt2Holder) {
      NullableUInt2Holder holder = (NullableUInt2Holder) h;
      UInt2Holder UInt2Holder = new UInt2Holder();
      UInt2Holder.value = holder.value;
      return UInt2Holder;
    }
    if (h instanceof NullableSmallIntHolder) {
      NullableSmallIntHolder holder = (NullableSmallIntHolder) h;
      SmallIntHolder SmallIntHolder = new SmallIntHolder();
      SmallIntHolder.value = holder.value;
      return SmallIntHolder;
    }
    if (h instanceof NullableIntHolder) {
      NullableIntHolder holder = (NullableIntHolder) h;
      IntHolder IntHolder = new IntHolder();
      IntHolder.value = holder.value;
      return IntHolder;
    }
    if (h instanceof NullableUInt4Holder) {
      NullableUInt4Holder holder = (NullableUInt4Holder) h;
      UInt4Holder UInt4Holder = new UInt4Holder();
      UInt4Holder.value = holder.value;
      return UInt4Holder;
    }
    if (h instanceof NullableFloat4Holder) {
      NullableFloat4Holder holder = (NullableFloat4Holder) h;
      Float4Holder Float4Holder = new Float4Holder();
      Float4Holder.value = holder.value;
      return Float4Holder;
    }
    if (h instanceof NullableIntervalYearHolder) {
      NullableIntervalYearHolder holder = (NullableIntervalYearHolder) h;
      IntervalYearHolder IntervalYearHolder = new IntervalYearHolder();
      IntervalYearHolder.value = holder.value;
      return IntervalYearHolder;
    }
    if (h instanceof NullableTimeMilliHolder) {
      NullableTimeMilliHolder holder = (NullableTimeMilliHolder) h;
      TimeMilliHolder TimeMilliHolder = new TimeMilliHolder();
      TimeMilliHolder.value = holder.value;
      return TimeMilliHolder;
    }
    if (h instanceof NullableBigIntHolder) {
      NullableBigIntHolder holder = (NullableBigIntHolder) h;
      BigIntHolder BigIntHolder = new BigIntHolder();
      BigIntHolder.value = holder.value;
      return BigIntHolder;
    }
    if (h instanceof NullableUInt8Holder) {
      NullableUInt8Holder holder = (NullableUInt8Holder) h;
      UInt8Holder UInt8Holder = new UInt8Holder();
      UInt8Holder.value = holder.value;
      return UInt8Holder;
    }
    if (h instanceof NullableFloat8Holder) {
      NullableFloat8Holder holder = (NullableFloat8Holder) h;
      Float8Holder Float8Holder = new Float8Holder();
      Float8Holder.value = holder.value;
      return Float8Holder;
    }
    if (h instanceof NullableDateMilliHolder) {
      NullableDateMilliHolder holder = (NullableDateMilliHolder) h;
      DateMilliHolder DateMilliHolder = new DateMilliHolder();
      DateMilliHolder.value = holder.value;
      return DateMilliHolder;
    }
    if (h instanceof NullableTimeStampMilliHolder) {
      NullableTimeStampMilliHolder holder = (NullableTimeStampMilliHolder) h;
      TimeStampMilliHolder TimeStampMilliHolder = new TimeStampMilliHolder();
      TimeStampMilliHolder.value = holder.value;
      return TimeStampMilliHolder;
    }
    if (h instanceof NullableIntervalDayHolder) {
      NullableIntervalDayHolder holder = (NullableIntervalDayHolder) h;
      IntervalDayHolder IntervalDayHolder = new IntervalDayHolder();
      IntervalDayHolder.days = holder.days;
      IntervalDayHolder.milliseconds = holder.milliseconds;
      return IntervalDayHolder;
    }
    if (h instanceof NullableDecimalHolder) {
      NullableDecimalHolder holder = (NullableDecimalHolder) h;
      DecimalHolder DecimalHolder = new DecimalHolder();
      DecimalHolder.start = holder.start;
      DecimalHolder.buffer = holder.buffer;
      DecimalHolder.scale = holder.scale;
      DecimalHolder.precision = holder.precision;
      return DecimalHolder;
    }
    if (h instanceof NullableFixedSizeBinaryHolder) {
      NullableFixedSizeBinaryHolder holder = (NullableFixedSizeBinaryHolder) h;
      FixedSizeBinaryHolder FixedSizeBinaryHolder = new FixedSizeBinaryHolder();
      FixedSizeBinaryHolder.buffer = holder.buffer;
      FixedSizeBinaryHolder.byteWidth = holder.byteWidth;
      return FixedSizeBinaryHolder;
    }
    if (h instanceof NullableVarBinaryHolder) {
      NullableVarBinaryHolder holder = (NullableVarBinaryHolder) h;
      VarBinaryHolder VarBinaryHolder = new VarBinaryHolder();
      VarBinaryHolder.start = holder.start;
      VarBinaryHolder.end = holder.end;
      VarBinaryHolder.buffer = holder.buffer;
      return VarBinaryHolder;
    }
    if (h instanceof NullableVarCharHolder) {
      NullableVarCharHolder holder = (NullableVarCharHolder) h;
      VarCharHolder VarCharHolder = new VarCharHolder();
      VarCharHolder.start = holder.start;
      VarCharHolder.end = holder.end;
      VarCharHolder.buffer = holder.buffer;
      return VarCharHolder;
    }
    if (h instanceof NullableBitHolder) {
      NullableBitHolder holder = (NullableBitHolder) h;
      BitHolder BitHolder = new BitHolder();
      BitHolder.value = holder.value;
      return BitHolder;
    }
    return h;
  }

  public static Field getFieldForSerializedField(SerializedField serializedField) {
    String name = serializedField.getNamePart().getName();
    org.apache.arrow.vector.types.Types.MinorType arrowMinorType = MajorTypeHelper
        .getArrowMinorType(serializedField.getMajorType().getMinorType());
    switch (serializedField.getMajorType().getMinorType()) {
    case LIST:
      return new Field(name, true, arrowMinorType.getType(),
          ImmutableList.of(getFieldForSerializedField(serializedField.getChild(2))));
    case STRUCT: {
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      List<SerializedField> childList = serializedField.getChildList();
      Preconditions.checkState(childList.size() > 0, "children should start with validity vector buffer");
      SerializedField bits = childList.get(0);
      Preconditions.checkState(bits.getNamePart().getName().equals("$bits$"),
          "children should start with validity vector buffer: %s", childList);
      for (int i = 1; i < childList.size(); i++) {
        SerializedField child = childList.get(i);
        builder.add(getFieldForSerializedField(child));
      }
      return new Field(name, true, arrowMinorType.getType(), builder.build());
    }
    case UNION: {
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      final List<SerializedField> unionChilds = serializedField.getChild(1).getChildList();
      final int typeIds[] = new int[unionChilds.size()];
      for (int i = 0; i < unionChilds.size(); i++) {
        final Field childField = getFieldForSerializedField(unionChilds.get(i));
        builder.add(childField);
        typeIds[i] = Types.getMinorTypeForArrowType(childField.getType()).ordinal();
      }

      // TODO: not sure the sparse mode is correct.
      final Union unionType = new Union(UnionMode.Sparse, typeIds);
      return new Field(name, true, unionType, builder.build());
    }
    case DECIMAL:
      return new Field(name, true,
          new Decimal(serializedField.getMajorType().getPrecision(), serializedField.getMajorType().getScale()), null);
    default:
      return new Field(name, true, arrowMinorType.getType(), null);
    }
  }
}
