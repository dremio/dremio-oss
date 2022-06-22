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
package org.apache.arrow.vector.util;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.impl.DateMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.DecimalWriterImpl;
import org.apache.arrow.vector.complex.impl.FixedSizeBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.impl.Float8WriterImpl;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalDayWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalYearWriterImpl;
import org.apache.arrow.vector.complex.impl.NullableBigIntHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableBitHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableDateMilliHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableDecimalHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableFixedSizeBinaryHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableFloat4HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableFloat8HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableIntHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableIntervalDayHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableIntervalYearHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableSmallIntHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.NullableTimeMilliHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableTimeStampMilliHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableTinyIntHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableUInt1HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableUInt2HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableUInt4HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableUInt8HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableVarBinaryHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableVarCharHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.SmallIntWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TinyIntWriterImpl;
import org.apache.arrow.vector.complex.impl.UInt1WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt2WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt4WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt8WriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.FixedSizeBinaryWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.IntervalDayWriter;
import org.apache.arrow.vector.complex.writer.IntervalYearWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.TinyIntWriter;
import org.apache.arrow.vector.complex.writer.UInt1Writer;
import org.apache.arrow.vector.complex.writer.UInt2Writer;
import org.apache.arrow.vector.complex.writer.UInt4Writer;
import org.apache.arrow.vector.complex.writer.UInt8Writer;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
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
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.ObjectType;
import com.dremio.exec.vector.ObjectVector;

/**
 * generated from BasicTypeHelper.java
 */
public class BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicTypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  // Default length when casting to varchar : 65536 = 2^16
  // This only defines an absolute maximum for values, setting
  // a high value like this will not inflate the size for small values
  public static final int VARCHAR_DEFAULT_CAST_LEN = 65536;

  protected static String buildErrorMessage(final String operation, final MinorType type) {
    return String.format("Unable to %s for minor type [%s]", operation, type);
  }

  public static int getSize(MinorType type) {
    switch (type) {
    case TINYINT:
      return 1;
    case UINT1:
      return 1;
    case UINT2:
      return 2;
    case SMALLINT:
      return 2;
    case INT:
      return 4;
    case UINT4:
      return 4;
    case FLOAT4:
      return 4;
    case INTERVALYEAR:
      return 4;
    case TIMEMILLI:
      return 4;
    case BIGINT:
      return 8;
    case UINT8:
      return 8;
    case FLOAT8:
      return 8;
    case DATEMILLI:
      return 8;
    case TIMESTAMPMILLI:
      return 8;
    case INTERVALDAY:
      return 8;
    case DECIMAL:
      return 16;
    case FIXEDSIZEBINARY:
      return -1 + WIDTH_ESTIMATE;
    case VARBINARY:
      return 4 + WIDTH_ESTIMATE;
    case VARCHAR:
      return 4 + WIDTH_ESTIMATE;
    case BIT:
      return 1;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get size", type));
  }

  public static Class<?> getValueVectorClass(MinorType type) {
    switch (type) {
    case UNION:
      return UnionVector.class;
    case STRUCT:
      return StructVector.class;
    case LIST:
      return ListVector.class;
    case NULL:
      return ZeroVector.class;
    case TINYINT:
      return TinyIntVector.class;
    case UINT1:
      return UInt1Vector.class;
    case UINT2:
      return UInt2Vector.class;
    case SMALLINT:
      return SmallIntVector.class;
    case INT:
      return IntVector.class;
    case UINT4:
      return UInt4Vector.class;
    case FLOAT4:
      return Float4Vector.class;
    case INTERVALYEAR:
      return IntervalYearVector.class;
    case TIMEMILLI:
      return TimeMilliVector.class;
    case BIGINT:
      return BigIntVector.class;
    case UINT8:
      return UInt8Vector.class;
    case FLOAT8:
      return Float8Vector.class;
    case DATEMILLI:
      return DateMilliVector.class;
    case TIMESTAMPMILLI:
      return TimeStampMilliVector.class;
    case INTERVALDAY:
      return IntervalDayVector.class;
    case DECIMAL:
      return DecimalVector.class;
    case FIXEDSIZEBINARY:
      return FixedSizeBinaryVector.class;
    case VARBINARY:
      return VarBinaryVector.class;
    case VARCHAR:
      return VarCharVector.class;
    case BIT:
      return BitVector.class;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get value vector class", type));
  }

  public static Class<?> getWriterInterface(MinorType type) {
    switch (type) {
    case UNION:
      return UnionWriter.class;
    case STRUCT:
      return StructWriter.class;
    case LIST:
      return ListWriter.class;
    case TINYINT:
      return TinyIntWriter.class;
    case UINT1:
      return UInt1Writer.class;
    case UINT2:
      return UInt2Writer.class;
    case SMALLINT:
      return SmallIntWriter.class;
    case INT:
      return IntWriter.class;
    case UINT4:
      return UInt4Writer.class;
    case FLOAT4:
      return Float4Writer.class;
    case INTERVALYEAR:
      return IntervalYearWriter.class;
    case TIMEMILLI:
      return TimeMilliWriter.class;
    case BIGINT:
      return BigIntWriter.class;
    case UINT8:
      return UInt8Writer.class;
    case FLOAT8:
      return Float8Writer.class;
    case DATEMILLI:
      return DateMilliWriter.class;
    case TIMESTAMPMILLI:
      return TimeStampMilliWriter.class;
    case INTERVALDAY:
      return IntervalDayWriter.class;
    case DECIMAL:
      return DecimalWriter.class;
    case FIXEDSIZEBINARY:
      return FixedSizeBinaryWriter.class;
    case VARBINARY:
      return VarBinaryWriter.class;
    case VARCHAR:
      return VarCharWriter.class;
    case BIT:
      return BitWriter.class;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get writer interface", type));
  }

  public static Class<?> getWriterImpl(MinorType type) {
    switch (type) {
    case UNION:
      return UnionWriter.class;
    case STRUCT:
      return NullableStructWriter.class;
    case LIST:
      return UnionListWriter.class;
    case TINYINT:
      return TinyIntWriterImpl.class;
    case UINT1:
      return UInt1WriterImpl.class;
    case UINT2:
      return UInt2WriterImpl.class;
    case SMALLINT:
      return SmallIntWriterImpl.class;
    case INT:
      return IntWriterImpl.class;
    case UINT4:
      return UInt4WriterImpl.class;
    case FLOAT4:
      return Float4WriterImpl.class;
    case INTERVALYEAR:
      return IntervalYearWriterImpl.class;
    case TIMEMILLI:
      return TimeMilliWriterImpl.class;
    case BIGINT:
      return BigIntWriterImpl.class;
    case UINT8:
      return UInt8WriterImpl.class;
    case FLOAT8:
      return Float8WriterImpl.class;
    case DATEMILLI:
      return DateMilliWriterImpl.class;
    case TIMESTAMPMILLI:
      return TimeStampMilliWriterImpl.class;
    case INTERVALDAY:
      return IntervalDayWriterImpl.class;
    case DECIMAL:
      return DecimalWriterImpl.class;
    case FIXEDSIZEBINARY:
      return FixedSizeBinaryWriterImpl.class;
    case VARBINARY:
      return VarBinaryWriterImpl.class;
    case VARCHAR:
      return VarCharWriterImpl.class;
    case BIT:
      return BitWriterImpl.class;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get writer implementation", type));
  }

  public static Class<?> getHolderReaderImpl(MinorType type) {
    switch (type) {
    case TINYINT:
      return NullableTinyIntHolderReaderImpl.class;
    case UINT1:
      return NullableUInt1HolderReaderImpl.class;
    case UINT2:
      return NullableUInt2HolderReaderImpl.class;
    case SMALLINT:
      return NullableSmallIntHolderReaderImpl.class;
    case INT:
      return NullableIntHolderReaderImpl.class;
    case UINT4:
      return NullableUInt4HolderReaderImpl.class;
    case FLOAT4:
      return NullableFloat4HolderReaderImpl.class;
    case INTERVALYEAR:
      return NullableIntervalYearHolderReaderImpl.class;
    case TIMEMILLI:
      return NullableTimeMilliHolderReaderImpl.class;
    case BIGINT:
      return NullableBigIntHolderReaderImpl.class;
    case UINT8:
      return NullableUInt8HolderReaderImpl.class;
    case FLOAT8:
      return NullableFloat8HolderReaderImpl.class;
    case DATEMILLI:
      return NullableDateMilliHolderReaderImpl.class;
    case TIMESTAMPMILLI:
      return NullableTimeStampMilliHolderReaderImpl.class;
    case INTERVALDAY:
      return NullableIntervalDayHolderReaderImpl.class;
    case DECIMAL:
      return NullableDecimalHolderReaderImpl.class;
    case FIXEDSIZEBINARY:
      return NullableFixedSizeBinaryHolderReaderImpl.class;
    case VARBINARY:
      return NullableVarBinaryHolderReaderImpl.class;
    case VARCHAR:
      return NullableVarCharHolderReaderImpl.class;
    case BIT:
      return NullableBitHolderReaderImpl.class;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get holder reader implementation", type));
  }

  public static FieldVector getNewVector(Field field, BufferAllocator allocator) {
    return getNewVector(field, allocator, null);
  }

  public static FieldVector getNewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    if (field.getType() instanceof ObjectType) {
      return new ObjectVector(field.getName(), allocator);
    }

    MinorType type = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());

    List<Field> children = field.getChildren();

    switch (type) {

    case UNION:
      UnionVector unionVector = new UnionVector(field.getName(), allocator, new FieldType(true, field.getType(), null), callBack);
      if (!children.isEmpty()) {
        unionVector.initializeChildrenFromFields(children);
      }
      return unionVector;
    case LIST:
      ListVector listVector = new ListVector(field.getName(), allocator, new FieldType(true, ArrowType.List.INSTANCE, null), callBack);
      if (!children.isEmpty()) {
        listVector.initializeChildrenFromFields(children);
      }
      return listVector;
    case STRUCT:
      StructVector structVector = new StructVector(field.getName(), allocator, new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
        callBack);
      if (!children.isEmpty()) {
        structVector.initializeChildrenFromFields(children);
      }
      return structVector;

    case NULL:
      return new ZeroVector(field);
    case TINYINT:
      return new TinyIntVector(field, allocator);
    case UINT1:
      return new UInt1Vector(field, allocator);
    case UINT2:
      return new UInt2Vector(field, allocator);
    case SMALLINT:
      return new SmallIntVector(field, allocator);
    case INT:
      return new IntVector(field, allocator);
    case UINT4:
      return new UInt4Vector(field, allocator);
    case FLOAT4:
      return new Float4Vector(field, allocator);
    case INTERVALYEAR:
      return new IntervalYearVector(field, allocator);
    case TIMEMILLI:
      return new TimeMilliVector(field, allocator);
    case BIGINT:
      return new BigIntVector(field, allocator);
    case UINT8:
      return new UInt8Vector(field, allocator);
    case FLOAT8:
      return new Float8Vector(field, allocator);
    case DATEMILLI:
      return new DateMilliVector(field, allocator);
    case TIMESTAMPMILLI:
      return new TimeStampMilliVector(field, allocator);
    case INTERVALDAY:
      return new IntervalDayVector(field, allocator);
    case DECIMAL:
      return new DecimalVector(field, allocator);
    case FIXEDSIZEBINARY:
      return new FixedSizeBinaryVector(field.getName(), allocator, WIDTH_ESTIMATE);
    case VARBINARY:
      return new VarBinaryVector(field, allocator);
    case VARCHAR:
      return new VarCharVector(field, allocator);
    case BIT:
      return new BitVector(field, allocator);
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(buildErrorMessage("get new vector", type));
  }

  public static com.dremio.common.types.TypeProtos.MajorType getValueHolderMajorType(
      Class<? extends ValueHolder> holderClass) {

    if (holderClass.equals(TinyIntHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.TINYINT);
    } else if (holderClass.equals(NullableTinyIntHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.TINYINT);
    } else if (holderClass.equals(UInt1Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT1);
    } else if (holderClass.equals(NullableUInt1Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UINT1);
    } else if (holderClass.equals(UInt2Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT2);
    } else if (holderClass.equals(NullableUInt2Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UINT2);
    } else if (holderClass.equals(SmallIntHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.SMALLINT);
    } else if (holderClass.equals(NullableSmallIntHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.SMALLINT);
    } else if (holderClass.equals(IntHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.INT);
    } else if (holderClass.equals(NullableIntHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.INT);
    } else if (holderClass.equals(UInt4Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT4);
    } else if (holderClass.equals(NullableUInt4Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UINT4);
    } else if (holderClass.equals(Float4Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.FLOAT4);
    } else if (holderClass.equals(NullableFloat4Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.FLOAT4);
    }
    // unsupported type DateDay
    else if (holderClass.equals(IntervalYearHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.INTERVALYEAR);
    } else if (holderClass.equals(NullableIntervalYearHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.INTERVALYEAR);
    }
    // unsupported type TimeSec
    else if (holderClass.equals(TimeMilliHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.TIME);
    } else if (holderClass.equals(NullableTimeMilliHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.TIME);
    } else if (holderClass.equals(BigIntHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.BIGINT);
    } else if (holderClass.equals(NullableBigIntHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.BIGINT);
    } else if (holderClass.equals(UInt8Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT8);
    } else if (holderClass.equals(NullableUInt8Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UINT8);
    } else if (holderClass.equals(Float8Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.FLOAT8);
    } else if (holderClass.equals(NullableFloat8Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.FLOAT8);
    } else if (holderClass.equals(DateMilliHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.DATE);
    } else if (holderClass.equals(NullableDateMilliHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.DATE);
    }
    // unsupported type Duration
    // unsupported type TimeStampSec
    else if (holderClass.equals(TimeStampMilliHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.TIMESTAMP);
    } else if (holderClass.equals(NullableTimeStampMilliHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.TIMESTAMP);
    }
    // unsupported type TimeStampMicro
    // unsupported type TimeStampNano
    // unsupported type TimeStampSecTZ
    // unsupported type TimeStampMilliTZ
    // unsupported type TimeStampMicroTZ
    // unsupported type TimeStampNanoTZ
    // unsupported type TimeMicro
    // unsupported type TimeNano
    else if (holderClass.equals(IntervalDayHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.INTERVALDAY);
    } else if (holderClass.equals(NullableIntervalDayHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.INTERVALDAY);
    } else if (holderClass.equals(DecimalHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.DECIMAL);
    } else if (holderClass.equals(NullableDecimalHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.DECIMAL);
    } else if (holderClass.equals(FixedSizeBinaryHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.FIXEDSIZEBINARY);
    } else if (holderClass.equals(NullableFixedSizeBinaryHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.FIXEDSIZEBINARY);
    } else if (holderClass.equals(VarBinaryHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.VARBINARY);
    } else if (holderClass.equals(NullableVarBinaryHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.VARBINARY);
    } else if (holderClass.equals(VarCharHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.VARCHAR);
    } else if (holderClass.equals(NullableVarCharHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.VARCHAR);
    } else if (holderClass.equals(BitHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.BIT);
    } else if (holderClass.equals(NullableBitHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.BIT);
    } else if (holderClass.equals(ObjectHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.GENERIC_OBJECT);
    } else if (holderClass.equals(UnionHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UNION);
    }

    throw new UnsupportedOperationException(
        String.format("%s is not supported for 'getValueHolderType' method.", holderClass.getName()));

  }

  public static ValueHolder getValueHolderForType(MinorType type) {

    switch (type) {
    case TINYINT:
      return new NullableTinyIntHolder();
    case UINT1:
      return new NullableUInt1Holder();
    case UINT2:
      return new NullableUInt2Holder();
    case SMALLINT:
      return new NullableSmallIntHolder();
    case INT:
      return new NullableIntHolder();
    case UINT4:
      return new NullableUInt4Holder();
    case FLOAT4:
      return new NullableFloat4Holder();
    case INTERVALYEAR:
      return new NullableIntervalYearHolder();
    case TIMEMILLI:
      return new NullableTimeMilliHolder();
    case BIGINT:
      return new NullableBigIntHolder();
    case UINT8:
      return new NullableUInt8Holder();
    case FLOAT8:
      return new NullableFloat8Holder();
    case DATEMILLI:
      return new NullableDateMilliHolder();
    case TIMESTAMPMILLI:
      return new NullableTimeStampMilliHolder();
    case INTERVALDAY:
      return new NullableIntervalDayHolder();
    case DECIMAL:
      return new NullableDecimalHolder();
    case FIXEDSIZEBINARY:
      return new NullableFixedSizeBinaryHolder();
    case VARBINARY:
      return new NullableVarBinaryHolder();
    case VARCHAR:
      return new NullableVarCharHolder();
    case BIT:
      return new NullableBitHolder();
    }
    throw new UnsupportedOperationException(
        String.format("%s is not supported for 'getValueHolderForType' method.", type));

  }

  public static MinorType getValueHolderType(ValueHolder holder) {

    if (0 == 1) {
      return null;
    } else if (holder instanceof TinyIntHolder) {
      return MinorType.TINYINT;
    } else if (holder instanceof NullableTinyIntHolder) {
      return MinorType.TINYINT;
    } else if (holder instanceof UInt1Holder) {
      return MinorType.UINT1;
    } else if (holder instanceof NullableUInt1Holder) {
      return MinorType.UINT1;
    } else if (holder instanceof UInt2Holder) {
      return MinorType.UINT2;
    } else if (holder instanceof NullableUInt2Holder) {
      return MinorType.UINT2;
    } else if (holder instanceof SmallIntHolder) {
      return MinorType.SMALLINT;
    } else if (holder instanceof NullableSmallIntHolder) {
      return MinorType.SMALLINT;
    } else if (holder instanceof IntHolder) {
      return MinorType.INT;
    } else if (holder instanceof NullableIntHolder) {
      return MinorType.INT;
    } else if (holder instanceof UInt4Holder) {
      return MinorType.UINT4;
    } else if (holder instanceof NullableUInt4Holder) {
      return MinorType.UINT4;
    } else if (holder instanceof Float4Holder) {
      return MinorType.FLOAT4;
    } else if (holder instanceof NullableFloat4Holder) {
      return MinorType.FLOAT4;
    } else if (holder instanceof IntervalYearHolder) {
      return MinorType.INTERVALYEAR;
    } else if (holder instanceof NullableIntervalYearHolder) {
      return MinorType.INTERVALYEAR;
    } else if (holder instanceof TimeMilliHolder) {
      return MinorType.TIMEMILLI;
    } else if (holder instanceof NullableTimeMilliHolder) {
      return MinorType.TIMEMILLI;
    } else if (holder instanceof BigIntHolder) {
      return MinorType.BIGINT;
    } else if (holder instanceof NullableBigIntHolder) {
      return MinorType.BIGINT;
    } else if (holder instanceof UInt8Holder) {
      return MinorType.UINT8;
    } else if (holder instanceof NullableUInt8Holder) {
      return MinorType.UINT8;
    } else if (holder instanceof Float8Holder) {
      return MinorType.FLOAT8;
    } else if (holder instanceof NullableFloat8Holder) {
      return MinorType.FLOAT8;
    } else if (holder instanceof DateMilliHolder) {
      return MinorType.DATEMILLI;
    } else if (holder instanceof NullableDateMilliHolder) {
      return MinorType.DATEMILLI;
    } else if (holder instanceof TimeStampMilliHolder) {
      return MinorType.TIMESTAMPMILLI;
    } else if (holder instanceof NullableTimeStampMilliHolder) {
      return MinorType.TIMESTAMPMILLI;
    } else if (holder instanceof IntervalDayHolder) {
      return MinorType.INTERVALDAY;
    } else if (holder instanceof NullableIntervalDayHolder) {
      return MinorType.INTERVALDAY;
    } else if (holder instanceof DecimalHolder) {
      return MinorType.DECIMAL;
    } else if (holder instanceof NullableDecimalHolder) {
      return MinorType.DECIMAL;
    } else if (holder instanceof FixedSizeBinaryHolder) {
      return MinorType.FIXEDSIZEBINARY;
    } else if (holder instanceof NullableFixedSizeBinaryHolder) {
      return MinorType.FIXEDSIZEBINARY;
    } else if (holder instanceof VarBinaryHolder) {
      return MinorType.VARBINARY;
    } else if (holder instanceof NullableVarBinaryHolder) {
      return MinorType.VARBINARY;
    } else if (holder instanceof VarCharHolder) {
      return MinorType.VARCHAR;
    } else if (holder instanceof NullableVarCharHolder) {
      return MinorType.VARCHAR;
    } else if (holder instanceof BitHolder) {
      return MinorType.BIT;
    } else if (holder instanceof NullableBitHolder) {
      return MinorType.BIT;
    }

    throw new UnsupportedOperationException("ValueHolder is not supported for 'getValueHolderType' method.");

  }

  public static void setNotNull(ValueHolder holder) {
    MinorType type = getValueHolderType(holder);

    switch (type) {
    case TINYINT:
      if (holder instanceof NullableTinyIntHolder) {
        ((NullableTinyIntHolder) holder).isSet = 1;
        return;
      }
    case UINT1:
      if (holder instanceof NullableUInt1Holder) {
        ((NullableUInt1Holder) holder).isSet = 1;
        return;
      }
    case UINT2:
      if (holder instanceof NullableUInt2Holder) {
        ((NullableUInt2Holder) holder).isSet = 1;
        return;
      }
    case SMALLINT:
      if (holder instanceof NullableSmallIntHolder) {
        ((NullableSmallIntHolder) holder).isSet = 1;
        return;
      }
    case INT:
      if (holder instanceof NullableIntHolder) {
        ((NullableIntHolder) holder).isSet = 1;
        return;
      }
    case UINT4:
      if (holder instanceof NullableUInt4Holder) {
        ((NullableUInt4Holder) holder).isSet = 1;
        return;
      }
    case FLOAT4:
      if (holder instanceof NullableFloat4Holder) {
        ((NullableFloat4Holder) holder).isSet = 1;
        return;
      }
    case INTERVALYEAR:
      if (holder instanceof NullableIntervalYearHolder) {
        ((NullableIntervalYearHolder) holder).isSet = 1;
        return;
      }
    case TIMEMILLI:
      if (holder instanceof NullableTimeMilliHolder) {
        ((NullableTimeMilliHolder) holder).isSet = 1;
        return;
      }
    case BIGINT:
      if (holder instanceof NullableBigIntHolder) {
        ((NullableBigIntHolder) holder).isSet = 1;
        return;
      }
    case UINT8:
      if (holder instanceof NullableUInt8Holder) {
        ((NullableUInt8Holder) holder).isSet = 1;
        return;
      }
    case FLOAT8:
      if (holder instanceof NullableFloat8Holder) {
        ((NullableFloat8Holder) holder).isSet = 1;
        return;
      }
    case DATEMILLI:
      if (holder instanceof NullableDateMilliHolder) {
        ((NullableDateMilliHolder) holder).isSet = 1;
        return;
      }
    case TIMESTAMPMILLI:
      if (holder instanceof NullableTimeStampMilliHolder) {
        ((NullableTimeStampMilliHolder) holder).isSet = 1;
        return;
      }
    case INTERVALDAY:
      if (holder instanceof NullableIntervalDayHolder) {
        ((NullableIntervalDayHolder) holder).isSet = 1;
        return;
      }
    case DECIMAL:
      if (holder instanceof NullableDecimalHolder) {
        ((NullableDecimalHolder) holder).isSet = 1;
        return;
      }
    case FIXEDSIZEBINARY:
      if (holder instanceof NullableFixedSizeBinaryHolder) {
        ((NullableFixedSizeBinaryHolder) holder).isSet = 1;
        return;
      }
    case VARBINARY:
      if (holder instanceof NullableVarBinaryHolder) {
        ((NullableVarBinaryHolder) holder).isSet = 1;
        return;
      }
    case VARCHAR:
      if (holder instanceof NullableVarCharHolder) {
        ((NullableVarCharHolder) holder).isSet = 1;
        return;
      }
    case BIT:
      if (holder instanceof NullableBitHolder) {
        ((NullableBitHolder) holder).isSet = 1;
        return;
      }
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set not null", type));
    }
  }

  public static boolean isNull(ValueHolder holder) {
    MinorType type = getValueHolderType(holder);

    switch (type) {
    case TINYINT:
      if (holder instanceof TinyIntHolder) {
        return false;
      } else {
        return ((NullableTinyIntHolder) holder).isSet == 0;
      }
    case UINT1:
      if (holder instanceof UInt1Holder) {
        return false;
      } else {
        return ((NullableUInt1Holder) holder).isSet == 0;
      }
    case UINT2:
      if (holder instanceof UInt2Holder) {
        return false;
      } else {
        return ((NullableUInt2Holder) holder).isSet == 0;
      }
    case SMALLINT:
      if (holder instanceof SmallIntHolder) {
        return false;
      } else {
        return ((NullableSmallIntHolder) holder).isSet == 0;
      }
    case INT:
      if (holder instanceof IntHolder) {
        return false;
      } else {
        return ((NullableIntHolder) holder).isSet == 0;
      }
    case UINT4:
      if (holder instanceof UInt4Holder) {
        return false;
      } else {
        return ((NullableUInt4Holder) holder).isSet == 0;
      }
    case FLOAT4:
      if (holder instanceof Float4Holder) {
        return false;
      } else {
        return ((NullableFloat4Holder) holder).isSet == 0;
      }
    case INTERVALYEAR:
      if (holder instanceof IntervalYearHolder) {
        return false;
      } else {
        return ((NullableIntervalYearHolder) holder).isSet == 0;
      }
    case TIMEMILLI:
      if (holder instanceof TimeMilliHolder) {
        return false;
      } else {
        return ((NullableTimeMilliHolder) holder).isSet == 0;
      }
    case BIGINT:
      if (holder instanceof BigIntHolder) {
        return false;
      } else {
        return ((NullableBigIntHolder) holder).isSet == 0;
      }
    case UINT8:
      if (holder instanceof UInt8Holder) {
        return false;
      } else {
        return ((NullableUInt8Holder) holder).isSet == 0;
      }
    case FLOAT8:
      if (holder instanceof Float8Holder) {
        return false;
      } else {
        return ((NullableFloat8Holder) holder).isSet == 0;
      }
    case DATEMILLI:
      if (holder instanceof DateMilliHolder) {
        return false;
      } else {
        return ((NullableDateMilliHolder) holder).isSet == 0;
      }
    case TIMESTAMPMILLI:
      if (holder instanceof TimeStampMilliHolder) {
        return false;
      } else {
        return ((NullableTimeStampMilliHolder) holder).isSet == 0;
      }
    case INTERVALDAY:
      if (holder instanceof IntervalDayHolder) {
        return false;
      } else {
        return ((NullableIntervalDayHolder) holder).isSet == 0;
      }
    case DECIMAL:
      if (holder instanceof DecimalHolder) {
        return false;
      } else {
        return ((NullableDecimalHolder) holder).isSet == 0;
      }
    case FIXEDSIZEBINARY:
      if (holder instanceof FixedSizeBinaryHolder) {
        return false;
      } else {
        return ((NullableFixedSizeBinaryHolder) holder).isSet == 0;
      }
    case VARBINARY:
      if (holder instanceof VarBinaryHolder) {
        return false;
      } else {
        return ((NullableVarBinaryHolder) holder).isSet == 0;
      }
    case VARCHAR:
      if (holder instanceof VarCharHolder) {
        return false;
      } else {
        return ((NullableVarCharHolder) holder).isSet == 0;
      }
    case BIT:
      if (holder instanceof BitHolder) {
        return false;
      } else {
        return ((NullableBitHolder) holder).isSet == 0;
      }
    default:
      throw new UnsupportedOperationException(buildErrorMessage("check is null", type));
    }
  }

  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = getMajorTypeForField(vector.getField());

    switch (type.getMinorType()) {
    case TINYINT:
      switch (type.getMode()) {
      case REQUIRED:
        ((TinyIntVector) vector).setSafe(index, (TinyIntHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableTinyIntHolder) {
          if (((NullableTinyIntHolder) holder).isSet == 1) {
            ((TinyIntVector) vector).setSafe(index, (NullableTinyIntHolder) holder);
          } else {
            ((TinyIntVector) vector).isSafe(index);
          }
        } else {
          ((TinyIntVector) vector).setSafe(index, (TinyIntHolder) holder);
        }
        return;
      }
    case UINT1:
      switch (type.getMode()) {
      case REQUIRED:
        ((UInt1Vector) vector).setSafe(index, (UInt1Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableUInt1Holder) {
          if (((NullableUInt1Holder) holder).isSet == 1) {
            ((UInt1Vector) vector).setSafe(index, (NullableUInt1Holder) holder);
          } else {
            ((UInt1Vector) vector).isSafe(index);
          }
        } else {
          ((UInt1Vector) vector).setSafe(index, (UInt1Holder) holder);
        }
        return;
      }
    case UINT2:
      switch (type.getMode()) {
      case REQUIRED:
        ((UInt2Vector) vector).setSafe(index, (UInt2Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableUInt2Holder) {
          if (((NullableUInt2Holder) holder).isSet == 1) {
            ((UInt2Vector) vector).setSafe(index, (NullableUInt2Holder) holder);
          } else {
            ((UInt2Vector) vector).isSafe(index);
          }
        } else {
          ((UInt2Vector) vector).setSafe(index, (UInt2Holder) holder);
        }
        return;
      }
    case SMALLINT:
      switch (type.getMode()) {
      case REQUIRED:
        ((SmallIntVector) vector).setSafe(index, (SmallIntHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableSmallIntHolder) {
          if (((NullableSmallIntHolder) holder).isSet == 1) {
            ((SmallIntVector) vector).setSafe(index, (NullableSmallIntHolder) holder);
          } else {
            ((SmallIntVector) vector).isSafe(index);
          }
        } else {
          ((SmallIntVector) vector).setSafe(index, (SmallIntHolder) holder);
        }
        return;
      }
    case INT:
      switch (type.getMode()) {
      case REQUIRED:
        ((IntVector) vector).setSafe(index, (IntHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableIntHolder) {
          if (((NullableIntHolder) holder).isSet == 1) {
            ((IntVector) vector).setSafe(index, (NullableIntHolder) holder);
          } else {
            ((IntVector) vector).isSafe(index);
          }
        } else {
          ((IntVector) vector).setSafe(index, (IntHolder) holder);
        }
        return;
      }
    case UINT4:
      switch (type.getMode()) {
      case REQUIRED:
        ((UInt4Vector) vector).setSafe(index, (UInt4Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableUInt4Holder) {
          if (((NullableUInt4Holder) holder).isSet == 1) {
            ((UInt4Vector) vector).setSafe(index, (NullableUInt4Holder) holder);
          } else {
            ((UInt4Vector) vector).isSafe(index);
          }
        } else {
          ((UInt4Vector) vector).setSafe(index, (UInt4Holder) holder);
        }
        return;
      }
    case FLOAT4:
      switch (type.getMode()) {
      case REQUIRED:
        ((Float4Vector) vector).setSafe(index, (Float4Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableFloat4Holder) {
          if (((NullableFloat4Holder) holder).isSet == 1) {
            ((Float4Vector) vector).setSafe(index, (NullableFloat4Holder) holder);
          } else {
            ((Float4Vector) vector).isSafe(index);
          }
        } else {
          ((Float4Vector) vector).setSafe(index, (Float4Holder) holder);
        }
        return;
      }
      // unsupported type DateDay
    case INTERVALYEAR:
      switch (type.getMode()) {
      case REQUIRED:
        ((IntervalYearVector) vector).setSafe(index, (IntervalYearHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableIntervalYearHolder) {
          if (((NullableIntervalYearHolder) holder).isSet == 1) {
            ((IntervalYearVector) vector).setSafe(index, (NullableIntervalYearHolder) holder);
          } else {
            ((IntervalYearVector) vector).isSafe(index);
          }
        } else {
          ((IntervalYearVector) vector).setSafe(index, (IntervalYearHolder) holder);
        }
        return;
      }
      // unsupported type TimeSec
    case TIME:
      switch (type.getMode()) {
      case REQUIRED:
        ((TimeMilliVector) vector).setSafe(index, (TimeMilliHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableTimeMilliHolder) {
          if (((NullableTimeMilliHolder) holder).isSet == 1) {
            ((TimeMilliVector) vector).setSafe(index, (NullableTimeMilliHolder) holder);
          } else {
            ((TimeMilliVector) vector).isSafe(index);
          }
        } else {
          ((TimeMilliVector) vector).setSafe(index, (TimeMilliHolder) holder);
        }
        return;
      }
    case BIGINT:
      switch (type.getMode()) {
      case REQUIRED:
        ((BigIntVector) vector).setSafe(index, (BigIntHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableBigIntHolder) {
          if (((NullableBigIntHolder) holder).isSet == 1) {
            ((BigIntVector) vector).setSafe(index, (NullableBigIntHolder) holder);
          } else {
            ((BigIntVector) vector).isSafe(index);
          }
        } else {
          ((BigIntVector) vector).setSafe(index, (BigIntHolder) holder);
        }
        return;
      }
    case UINT8:
      switch (type.getMode()) {
      case REQUIRED:
        ((UInt8Vector) vector).setSafe(index, (UInt8Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableUInt8Holder) {
          if (((NullableUInt8Holder) holder).isSet == 1) {
            ((UInt8Vector) vector).setSafe(index, (NullableUInt8Holder) holder);
          } else {
            ((UInt8Vector) vector).isSafe(index);
          }
        } else {
          ((UInt8Vector) vector).setSafe(index, (UInt8Holder) holder);
        }
        return;
      }
    case FLOAT8:
      switch (type.getMode()) {
      case REQUIRED:
        ((Float8Vector) vector).setSafe(index, (Float8Holder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableFloat8Holder) {
          if (((NullableFloat8Holder) holder).isSet == 1) {
            ((Float8Vector) vector).setSafe(index, (NullableFloat8Holder) holder);
          } else {
            ((Float8Vector) vector).isSafe(index);
          }
        } else {
          ((Float8Vector) vector).setSafe(index, (Float8Holder) holder);
        }
        return;
      }
    case DATE:
      switch (type.getMode()) {
      case REQUIRED:
        ((DateMilliVector) vector).setSafe(index, (DateMilliHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableDateMilliHolder) {
          if (((NullableDateMilliHolder) holder).isSet == 1) {
            ((DateMilliVector) vector).setSafe(index, (NullableDateMilliHolder) holder);
          } else {
            ((DateMilliVector) vector).isSafe(index);
          }
        } else {
          ((DateMilliVector) vector).setSafe(index, (DateMilliHolder) holder);
        }
        return;
      }
      // unsupported type Duration
      // unsupported type TimeStampSec
    case TIMESTAMP:
      switch (type.getMode()) {
      case REQUIRED:
        ((TimeStampMilliVector) vector).setSafe(index, (TimeStampMilliHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableTimeStampMilliHolder) {
          if (((NullableTimeStampMilliHolder) holder).isSet == 1) {
            ((TimeStampMilliVector) vector).setSafe(index, (NullableTimeStampMilliHolder) holder);
          } else {
            ((TimeStampMilliVector) vector).isSafe(index);
          }
        } else {
          ((TimeStampMilliVector) vector).setSafe(index, (TimeStampMilliHolder) holder);
        }
        return;
      }
      // unsupported type TimeStampMicro
      // unsupported type TimeStampNano
      // unsupported type TimeStampSecTZ
      // unsupported type TimeStampMilliTZ
      // unsupported type TimeStampMicroTZ
      // unsupported type TimeStampNanoTZ
      // unsupported type TimeMicro
      // unsupported type TimeNano
    case INTERVALDAY:
      switch (type.getMode()) {
      case REQUIRED:
        ((IntervalDayVector) vector).setSafe(index, (IntervalDayHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableIntervalDayHolder) {
          if (((NullableIntervalDayHolder) holder).isSet == 1) {
            ((IntervalDayVector) vector).setSafe(index, (NullableIntervalDayHolder) holder);
          } else {
            ((IntervalDayVector) vector).isSafe(index);
          }
        } else {
          ((IntervalDayVector) vector).setSafe(index, (IntervalDayHolder) holder);
        }
        return;
      }
    case DECIMAL:
      switch (type.getMode()) {
      case REQUIRED:
        ((DecimalVector) vector).setSafe(index, (DecimalHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableDecimalHolder) {
          if (((NullableDecimalHolder) holder).isSet == 1) {
            ((DecimalVector) vector).setSafe(index, (NullableDecimalHolder) holder);
          } else {
            ((DecimalVector) vector).isSafe(index);
          }
        } else {
          ((DecimalVector) vector).setSafe(index, (DecimalHolder) holder);
        }
        return;
      }
    case FIXEDSIZEBINARY:
      switch (type.getMode()) {
      case REQUIRED:
        ((FixedSizeBinaryVector) vector).setSafe(index, (FixedSizeBinaryHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableFixedSizeBinaryHolder) {
          if (((NullableFixedSizeBinaryHolder) holder).isSet == 1) {
            ((FixedSizeBinaryVector) vector).setSafe(index, (NullableFixedSizeBinaryHolder) holder);
          } else {
            ((FixedSizeBinaryVector) vector).isSafe(index);
          }
        } else {
          ((FixedSizeBinaryVector) vector).setSafe(index, (FixedSizeBinaryHolder) holder);
        }
        return;
      }
    case VARBINARY:
      switch (type.getMode()) {
      case REQUIRED:
        ((VarBinaryVector) vector).setSafe(index, (VarBinaryHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableVarBinaryHolder) {
          if (((NullableVarBinaryHolder) holder).isSet == 1) {
            ((VarBinaryVector) vector).setSafe(index, (NullableVarBinaryHolder) holder);
          } else {
            ((VarBinaryVector) vector).isSafe(index);
          }
        } else {
          ((VarBinaryVector) vector).setSafe(index, (VarBinaryHolder) holder);
        }
        return;
      }
    case VARCHAR:
      switch (type.getMode()) {
      case REQUIRED:
        ((VarCharVector) vector).setSafe(index, (VarCharHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableVarCharHolder) {
          if (((NullableVarCharHolder) holder).isSet == 1) {
            ((VarCharVector) vector).setSafe(index, (NullableVarCharHolder) holder);
          } else {
            ((VarCharVector) vector).isSafe(index);
          }
        } else {
          ((VarCharVector) vector).setSafe(index, (VarCharHolder) holder);
        }
        return;
      }
    case BIT:
      switch (type.getMode()) {
      case REQUIRED:
        ((BitVector) vector).setSafe(index, (BitHolder) holder);
        return;
      case OPTIONAL:
        if (holder instanceof NullableBitHolder) {
          if (((NullableBitHolder) holder).isSet == 1) {
            ((BitVector) vector).setSafe(index, (NullableBitHolder) holder);
          } else {
            ((BitVector) vector).isSafe(index);
          }
        } else {
          ((BitVector) vector).setSafe(index, (BitHolder) holder);
        }
        return;
      }
    case GENERIC_OBJECT:
      ((ObjectVector) vector).setSafe(index, (ObjectHolder) holder);
    default:
      throw new UnsupportedOperationException(
          buildErrorMessage("set value safe", getArrowMinorType(type.getMinorType())));
    }
  }

  public static ValueHolder createValueHolder(MinorType type) {
    switch (type) {
    case TINYINT:
      return new NullableTinyIntHolder();
    case UINT1:
      return new NullableUInt1Holder();
    case UINT2:
      return new NullableUInt2Holder();
    case SMALLINT:
      return new NullableSmallIntHolder();
    case INT:
      return new NullableIntHolder();
    case UINT4:
      return new NullableUInt4Holder();
    case FLOAT4:
      return new NullableFloat4Holder();
    case INTERVALYEAR:
      return new NullableIntervalYearHolder();
    case TIMEMILLI:
      return new NullableTimeMilliHolder();
    case BIGINT:
      return new NullableBigIntHolder();
    case UINT8:
      return new NullableUInt8Holder();
    case FLOAT8:
      return new NullableFloat8Holder();
    case DATEMILLI:
      return new NullableDateMilliHolder();
    case TIMESTAMPMILLI:
      return new NullableTimeStampMilliHolder();
    case INTERVALDAY:
      return new NullableIntervalDayHolder();
    case DECIMAL:
      return new NullableDecimalHolder();
    case FIXEDSIZEBINARY:
      return new NullableFixedSizeBinaryHolder();
    case VARBINARY:
      return new NullableVarBinaryHolder();
    case VARCHAR:
      return new NullableVarCharHolder();
    case BIT:
      return new NullableBitHolder();
    default:
      throw new UnsupportedOperationException(buildErrorMessage("create value holder", type));
    }
  }

  public static ValueHolder getValue(ValueVector vector, int index) {
    MinorType type = getMinorTypeForArrowType(vector.getField().getType());
    ValueHolder holder;
    switch (type) {
    case TINYINT:
      holder = new NullableTinyIntHolder();
      ((NullableTinyIntHolder) holder).isSet = ((TinyIntVector) vector).isSet(index);
      if (((NullableTinyIntHolder) holder).isSet == 1) {
        ((NullableTinyIntHolder) holder).value = ((TinyIntVector) vector).get(index);
      }
      return holder;
    case UINT1:
      holder = new NullableUInt1Holder();
      ((NullableUInt1Holder) holder).isSet = ((UInt1Vector) vector).isSet(index);
      if (((NullableUInt1Holder) holder).isSet == 1) {
        ((NullableUInt1Holder) holder).value = ((UInt1Vector) vector).get(index);
      }
      return holder;
    case UINT2:
      holder = new NullableUInt2Holder();
      ((NullableUInt2Holder) holder).isSet = ((UInt2Vector) vector).isSet(index);
      if (((NullableUInt2Holder) holder).isSet == 1) {
        ((NullableUInt2Holder) holder).value = ((UInt2Vector) vector).get(index);
      }
      return holder;
    case SMALLINT:
      holder = new NullableSmallIntHolder();
      ((NullableSmallIntHolder) holder).isSet = ((SmallIntVector) vector).isSet(index);
      if (((NullableSmallIntHolder) holder).isSet == 1) {
        ((NullableSmallIntHolder) holder).value = ((SmallIntVector) vector).get(index);
      }
      return holder;
    case INT:
      holder = new NullableIntHolder();
      ((NullableIntHolder) holder).isSet = ((IntVector) vector).isSet(index);
      if (((NullableIntHolder) holder).isSet == 1) {
        ((NullableIntHolder) holder).value = ((IntVector) vector).get(index);
      }
      return holder;
    case UINT4:
      holder = new NullableUInt4Holder();
      ((NullableUInt4Holder) holder).isSet = ((UInt4Vector) vector).isSet(index);
      if (((NullableUInt4Holder) holder).isSet == 1) {
        ((NullableUInt4Holder) holder).value = ((UInt4Vector) vector).get(index);
      }
      return holder;
    case FLOAT4:
      holder = new NullableFloat4Holder();
      ((NullableFloat4Holder) holder).isSet = ((Float4Vector) vector).isSet(index);
      if (((NullableFloat4Holder) holder).isSet == 1) {
        ((NullableFloat4Holder) holder).value = ((Float4Vector) vector).get(index);
      }
      return holder;
    case INTERVALYEAR:
      holder = new NullableIntervalYearHolder();
      ((NullableIntervalYearHolder) holder).isSet = ((IntervalYearVector) vector).isSet(index);
      if (((NullableIntervalYearHolder) holder).isSet == 1) {
        ((NullableIntervalYearHolder) holder).value = ((IntervalYearVector) vector).get(index);
      }
      return holder;
    case TIMEMILLI:
      holder = new NullableTimeMilliHolder();
      ((NullableTimeMilliHolder) holder).isSet = ((TimeMilliVector) vector).isSet(index);
      if (((NullableTimeMilliHolder) holder).isSet == 1) {
        ((NullableTimeMilliHolder) holder).value = ((TimeMilliVector) vector).get(index);
      }
      return holder;
    case BIGINT:
      holder = new NullableBigIntHolder();
      ((NullableBigIntHolder) holder).isSet = ((BigIntVector) vector).isSet(index);
      if (((NullableBigIntHolder) holder).isSet == 1) {
        ((NullableBigIntHolder) holder).value = ((BigIntVector) vector).get(index);
      }
      return holder;
    case UINT8:
      holder = new NullableUInt8Holder();
      ((NullableUInt8Holder) holder).isSet = ((UInt8Vector) vector).isSet(index);
      if (((NullableUInt8Holder) holder).isSet == 1) {
        ((NullableUInt8Holder) holder).value = ((UInt8Vector) vector).get(index);
      }
      return holder;
    case FLOAT8:
      holder = new NullableFloat8Holder();
      ((NullableFloat8Holder) holder).isSet = ((Float8Vector) vector).isSet(index);
      if (((NullableFloat8Holder) holder).isSet == 1) {
        ((NullableFloat8Holder) holder).value = ((Float8Vector) vector).get(index);
      }
      return holder;
    case DATEMILLI:
      holder = new NullableDateMilliHolder();
      ((NullableDateMilliHolder) holder).isSet = ((DateMilliVector) vector).isSet(index);
      if (((NullableDateMilliHolder) holder).isSet == 1) {
        ((NullableDateMilliHolder) holder).value = ((DateMilliVector) vector).get(index);
      }
      return holder;
    case TIMESTAMPMILLI:
      holder = new NullableTimeStampMilliHolder();
      ((NullableTimeStampMilliHolder) holder).isSet = ((TimeStampMilliVector) vector).isSet(index);
      if (((NullableTimeStampMilliHolder) holder).isSet == 1) {
        ((NullableTimeStampMilliHolder) holder).value = ((TimeStampMilliVector) vector).get(index);
      }
      return holder;
    case INTERVALDAY:
      holder = new NullableIntervalDayHolder();
      ((NullableIntervalDayHolder) holder).isSet = ((IntervalDayVector) vector).isSet(index);
      if (((NullableIntervalDayHolder) holder).isSet == 1) {
        ((IntervalDayVector) vector).get(index, (NullableIntervalDayHolder) holder);
      }
      return holder;
    case DECIMAL:
      holder = new NullableDecimalHolder();
      ((NullableDecimalHolder) holder).isSet = ((DecimalVector) vector).isSet(index);
      if (((NullableDecimalHolder) holder).isSet == 1) {
        ((DecimalVector) vector).get(index, (NullableDecimalHolder) holder);
      }
      return holder;
    case FIXEDSIZEBINARY:
      holder = new NullableFixedSizeBinaryHolder();
      ((NullableFixedSizeBinaryHolder) holder).isSet = ((FixedSizeBinaryVector) vector).isSet(index);
      if (((NullableFixedSizeBinaryHolder) holder).isSet == 1) {
        ((FixedSizeBinaryVector) vector).get(index, (NullableFixedSizeBinaryHolder) holder);
      }
      return holder;
    case VARBINARY:
      holder = new NullableVarBinaryHolder();
      ((NullableVarBinaryHolder) holder).isSet = ((VarBinaryVector) vector).isSet(index);
      if (((NullableVarBinaryHolder) holder).isSet == 1) {
        ((VarBinaryVector) vector).get(index, (NullableVarBinaryHolder) holder);
      }
      return holder;
    case VARCHAR:
      holder = new NullableVarCharHolder();
      ((NullableVarCharHolder) holder).isSet = ((VarCharVector) vector).isSet(index);
      if (((NullableVarCharHolder) holder).isSet == 1) {
        ((VarCharVector) vector).get(index, (NullableVarCharHolder) holder);
      }
      return holder;
    case BIT:
      holder = new NullableBitHolder();
      ((NullableBitHolder) holder).isSet = ((BitVector) vector).isSet(index);
      if (((NullableBitHolder) holder).isSet == 1) {
        ((NullableBitHolder) holder).value = ((BitVector) vector).get(index);
      }
      return holder;
    }

    throw new UnsupportedOperationException(buildErrorMessage("get value", type));
  }

  public static ValueHolder deNullify(ValueHolder holder) {
    MajorType type = getValueHolderMajorType(holder.getClass());

    switch (type.getMinorType()) {
    case TINYINT:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableTinyIntHolder) holder).isSet == 1) {
          TinyIntHolder newHolder = new TinyIntHolder();

          newHolder.value = ((NullableTinyIntHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case UINT1:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableUInt1Holder) holder).isSet == 1) {
          UInt1Holder newHolder = new UInt1Holder();

          newHolder.value = ((NullableUInt1Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case UINT2:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableUInt2Holder) holder).isSet == 1) {
          UInt2Holder newHolder = new UInt2Holder();

          newHolder.value = ((NullableUInt2Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case SMALLINT:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableSmallIntHolder) holder).isSet == 1) {
          SmallIntHolder newHolder = new SmallIntHolder();

          newHolder.value = ((NullableSmallIntHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case INT:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableIntHolder) holder).isSet == 1) {
          IntHolder newHolder = new IntHolder();

          newHolder.value = ((NullableIntHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case UINT4:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableUInt4Holder) holder).isSet == 1) {
          UInt4Holder newHolder = new UInt4Holder();

          newHolder.value = ((NullableUInt4Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case FLOAT4:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableFloat4Holder) holder).isSet == 1) {
          Float4Holder newHolder = new Float4Holder();

          newHolder.value = ((NullableFloat4Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
      // unsupported type DateDay
    case INTERVALYEAR:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableIntervalYearHolder) holder).isSet == 1) {
          IntervalYearHolder newHolder = new IntervalYearHolder();

          newHolder.value = ((NullableIntervalYearHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
      // unsupported type TimeSec
    case TIME:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableTimeMilliHolder) holder).isSet == 1) {
          TimeMilliHolder newHolder = new TimeMilliHolder();

          newHolder.value = ((NullableTimeMilliHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case BIGINT:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableBigIntHolder) holder).isSet == 1) {
          BigIntHolder newHolder = new BigIntHolder();

          newHolder.value = ((NullableBigIntHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case UINT8:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableUInt8Holder) holder).isSet == 1) {
          UInt8Holder newHolder = new UInt8Holder();

          newHolder.value = ((NullableUInt8Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case FLOAT8:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableFloat8Holder) holder).isSet == 1) {
          Float8Holder newHolder = new Float8Holder();

          newHolder.value = ((NullableFloat8Holder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case DATE:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableDateMilliHolder) holder).isSet == 1) {
          DateMilliHolder newHolder = new DateMilliHolder();

          newHolder.value = ((NullableDateMilliHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
      // unsupported type Duration
      // unsupported type TimeStampSec
    case TIMESTAMP:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableTimeStampMilliHolder) holder).isSet == 1) {
          TimeStampMilliHolder newHolder = new TimeStampMilliHolder();

          newHolder.value = ((NullableTimeStampMilliHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
      // unsupported type TimeStampMicro
      // unsupported type TimeStampNano
      // unsupported type TimeStampSecTZ
      // unsupported type TimeStampMilliTZ
      // unsupported type TimeStampMicroTZ
      // unsupported type TimeStampNanoTZ
      // unsupported type TimeMicro
      // unsupported type TimeNano
    case INTERVALDAY:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableIntervalDayHolder) holder).isSet == 1) {
          IntervalDayHolder newHolder = new IntervalDayHolder();

          newHolder.days = ((NullableIntervalDayHolder) holder).days;
          newHolder.milliseconds = ((NullableIntervalDayHolder) holder).milliseconds;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case DECIMAL:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableDecimalHolder) holder).isSet == 1) {
          DecimalHolder newHolder = new DecimalHolder();

          newHolder.start = ((NullableDecimalHolder) holder).start;
          newHolder.buffer = ((NullableDecimalHolder) holder).buffer;
          newHolder.scale = ((NullableDecimalHolder) holder).scale;
          newHolder.precision = ((NullableDecimalHolder) holder).precision;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case FIXEDSIZEBINARY:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableFixedSizeBinaryHolder) holder).isSet == 1) {
          FixedSizeBinaryHolder newHolder = new FixedSizeBinaryHolder();

          newHolder.buffer = ((NullableFixedSizeBinaryHolder) holder).buffer;
          newHolder.byteWidth = ((NullableFixedSizeBinaryHolder) holder).byteWidth;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case VARBINARY:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableVarBinaryHolder) holder).isSet == 1) {
          VarBinaryHolder newHolder = new VarBinaryHolder();

          newHolder.start = ((NullableVarBinaryHolder) holder).start;
          newHolder.end = ((NullableVarBinaryHolder) holder).end;
          newHolder.buffer = ((NullableVarBinaryHolder) holder).buffer;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case VARCHAR:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableVarCharHolder) holder).isSet == 1) {
          VarCharHolder newHolder = new VarCharHolder();

          newHolder.start = ((NullableVarCharHolder) holder).start;
          newHolder.end = ((NullableVarCharHolder) holder).end;
          newHolder.buffer = ((NullableVarCharHolder) holder).buffer;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    case BIT:

      switch (type.getMode()) {
      case REQUIRED:
        return holder;
      case OPTIONAL:
        if (((NullableBitHolder) holder).isSet == 1) {
          BitHolder newHolder = new BitHolder();

          newHolder.value = ((NullableBitHolder) holder).value;

          return newHolder;
        } else {
          throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
        }
      case REPEATED:
        return holder;
      }
    default:
      throw new UnsupportedOperationException(buildErrorMessage("deNullify", getArrowMinorType(type.getMinorType())));
    }
  }

  public static ValueHolder nullify(ValueHolder holder) {
    MajorType type = getValueHolderMajorType(holder.getClass());

    switch (type.getMinorType()) {
    case TINYINT:
      switch (type.getMode()) {
      case REQUIRED:
        NullableTinyIntHolder newHolder = new NullableTinyIntHolder();
        newHolder.isSet = 1;
        newHolder.value = ((TinyIntHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case UINT1:
      switch (type.getMode()) {
      case REQUIRED:
        NullableUInt1Holder newHolder = new NullableUInt1Holder();
        newHolder.isSet = 1;
        newHolder.value = ((UInt1Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case UINT2:
      switch (type.getMode()) {
      case REQUIRED:
        NullableUInt2Holder newHolder = new NullableUInt2Holder();
        newHolder.isSet = 1;
        newHolder.value = ((UInt2Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case SMALLINT:
      switch (type.getMode()) {
      case REQUIRED:
        NullableSmallIntHolder newHolder = new NullableSmallIntHolder();
        newHolder.isSet = 1;
        newHolder.value = ((SmallIntHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case INT:
      switch (type.getMode()) {
      case REQUIRED:
        NullableIntHolder newHolder = new NullableIntHolder();
        newHolder.isSet = 1;
        newHolder.value = ((IntHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case UINT4:
      switch (type.getMode()) {
      case REQUIRED:
        NullableUInt4Holder newHolder = new NullableUInt4Holder();
        newHolder.isSet = 1;
        newHolder.value = ((UInt4Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case FLOAT4:
      switch (type.getMode()) {
      case REQUIRED:
        NullableFloat4Holder newHolder = new NullableFloat4Holder();
        newHolder.isSet = 1;
        newHolder.value = ((Float4Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
      // unsupported type DateDay
    case INTERVALYEAR:
      switch (type.getMode()) {
      case REQUIRED:
        NullableIntervalYearHolder newHolder = new NullableIntervalYearHolder();
        newHolder.isSet = 1;
        newHolder.value = ((IntervalYearHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
      // unsupported type TimeSec
    case TIME:
      switch (type.getMode()) {
      case REQUIRED:
        NullableTimeMilliHolder newHolder = new NullableTimeMilliHolder();
        newHolder.isSet = 1;
        newHolder.value = ((TimeMilliHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case BIGINT:
      switch (type.getMode()) {
      case REQUIRED:
        NullableBigIntHolder newHolder = new NullableBigIntHolder();
        newHolder.isSet = 1;
        newHolder.value = ((BigIntHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case UINT8:
      switch (type.getMode()) {
      case REQUIRED:
        NullableUInt8Holder newHolder = new NullableUInt8Holder();
        newHolder.isSet = 1;
        newHolder.value = ((UInt8Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case FLOAT8:
      switch (type.getMode()) {
      case REQUIRED:
        NullableFloat8Holder newHolder = new NullableFloat8Holder();
        newHolder.isSet = 1;
        newHolder.value = ((Float8Holder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case DATE:
      switch (type.getMode()) {
      case REQUIRED:
        NullableDateMilliHolder newHolder = new NullableDateMilliHolder();
        newHolder.isSet = 1;
        newHolder.value = ((DateMilliHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
      // unsupported type Duration
      // unsupported type TimeStampSec
    case TIMESTAMP:
      switch (type.getMode()) {
      case REQUIRED:
        NullableTimeStampMilliHolder newHolder = new NullableTimeStampMilliHolder();
        newHolder.isSet = 1;
        newHolder.value = ((TimeStampMilliHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
      // unsupported type TimeStampMicro
      // unsupported type TimeStampNano
      // unsupported type TimeStampSecTZ
      // unsupported type TimeStampMilliTZ
      // unsupported type TimeStampMicroTZ
      // unsupported type TimeStampNanoTZ
      // unsupported type TimeMicro
      // unsupported type TimeNano
    case INTERVALDAY:
      switch (type.getMode()) {
      case REQUIRED:
        NullableIntervalDayHolder newHolder = new NullableIntervalDayHolder();
        newHolder.isSet = 1;
        newHolder.days = ((IntervalDayHolder) holder).days;
        newHolder.milliseconds = ((IntervalDayHolder) holder).milliseconds;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case DECIMAL:
      switch (type.getMode()) {
      case REQUIRED:
        NullableDecimalHolder newHolder = new NullableDecimalHolder();
        newHolder.isSet = 1;
        newHolder.start = ((DecimalHolder) holder).start;
        newHolder.buffer = ((DecimalHolder) holder).buffer;
        newHolder.scale = ((DecimalHolder) holder).scale;
        newHolder.precision = ((DecimalHolder) holder).precision;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case FIXEDSIZEBINARY:
      switch (type.getMode()) {
      case REQUIRED:
        NullableFixedSizeBinaryHolder newHolder = new NullableFixedSizeBinaryHolder();
        newHolder.isSet = 1;
        newHolder.buffer = ((FixedSizeBinaryHolder) holder).buffer;
        newHolder.byteWidth = ((FixedSizeBinaryHolder) holder).byteWidth;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case VARBINARY:
      switch (type.getMode()) {
      case REQUIRED:
        NullableVarBinaryHolder newHolder = new NullableVarBinaryHolder();
        newHolder.isSet = 1;
        newHolder.start = ((VarBinaryHolder) holder).start;
        newHolder.end = ((VarBinaryHolder) holder).end;
        newHolder.buffer = ((VarBinaryHolder) holder).buffer;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case VARCHAR:
      switch (type.getMode()) {
      case REQUIRED:
        NullableVarCharHolder newHolder = new NullableVarCharHolder();
        newHolder.isSet = 1;
        newHolder.start = ((VarCharHolder) holder).start;
        newHolder.end = ((VarCharHolder) holder).end;
        newHolder.buffer = ((VarCharHolder) holder).buffer;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    case BIT:
      switch (type.getMode()) {
      case REQUIRED:
        NullableBitHolder newHolder = new NullableBitHolder();
        newHolder.isSet = 1;
        newHolder.value = ((BitHolder) holder).value;
        return newHolder;
      case OPTIONAL:
        return holder;
      case REPEATED:
        throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
      }
    default:
      throw new UnsupportedOperationException(buildErrorMessage("nullify", getArrowMinorType(type.getMinorType())));
    }
  }

}
