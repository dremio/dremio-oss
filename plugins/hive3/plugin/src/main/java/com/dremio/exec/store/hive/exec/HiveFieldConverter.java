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
package com.dremio.exec.store.hive.exec;

import static com.dremio.exec.store.hive.HiveUtilities.getTrueEpochInMillis;
import static com.dremio.exec.store.hive.HiveUtilities.throwUnsupportedHiveDataTypeError;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.hive.exec.HiveAbstractReader.HiveOperatorContextOptions;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

public abstract class HiveFieldConverter {
  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HiveFieldConverter.class);

  public abstract void setSafeValue(
      ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex);

  private static Map<PrimitiveCategory, Class<? extends HiveFieldConverter>> primMap =
      Maps.newHashMap();
  protected HiveOperatorContextOptions contextOptions;

  public HiveFieldConverter(HiveOperatorContextOptions options) {
    this.contextOptions = options;
  }

  protected void checkSizeLimit(int size) {
    FieldSizeLimitExceptionHelper.checkSizeLimit(size, contextOptions.getMaxCellSize(), logger);
  }

  // TODO (DRILL-2470)
  // Byte and short (tinyint and smallint in SQL types) are currently read as integers
  // as these smaller integer types are not fully supported in Dremio today.
  // Here the same types are used, as we have to read out of the correct typed converter
  // from the hive side, in the FieldConverter classes below for Byte and Short we convert
  // to integer when writing into Dremio's vectors.
  static {
    primMap.put(PrimitiveCategory.BINARY, Binary.class);
    primMap.put(PrimitiveCategory.BOOLEAN, Boolean.class);
    primMap.put(PrimitiveCategory.BYTE, Byte.class);
    primMap.put(PrimitiveCategory.DOUBLE, Double.class);
    primMap.put(PrimitiveCategory.FLOAT, Float.class);
    primMap.put(PrimitiveCategory.INT, Int.class);
    primMap.put(PrimitiveCategory.LONG, Long.class);
    primMap.put(PrimitiveCategory.SHORT, Short.class);
    primMap.put(PrimitiveCategory.STRING, String.class);
    primMap.put(PrimitiveCategory.VARCHAR, VarChar.class);
    primMap.put(PrimitiveCategory.TIMESTAMP, Timestamp.class);
    primMap.put(PrimitiveCategory.DATE, Date.class);
    primMap.put(PrimitiveCategory.CHAR, Char.class);
  }

  public static HiveFieldConverter create(
      TypeInfo typeInfo, OperatorContext context, HiveOperatorContextOptions options)
      throws IllegalAccessException,
          InstantiationException,
          NoSuchMethodException,
          InvocationTargetException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveCategory pCat = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        if (pCat != PrimitiveCategory.DECIMAL) {
          Class<? extends HiveFieldConverter> clazz = primMap.get(pCat);
          if (clazz != null) {
            return clazz.getConstructor(HiveOperatorContextOptions.class).newInstance(options);
          }
        } else {
          // For decimal, based on precision return appropriate converter.
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
          int precision = decimalTypeInfo.precision();
          int scale = decimalTypeInfo.scale();
          return new Decimal(precision, scale, context, options);
        }

        throwUnsupportedHiveDataTypeError(pCat.toString());
        break;

      case LIST:
        {
          return new HiveList((ListTypeInfo) typeInfo, context, options);
        }
      case STRUCT:
        {
          return new HiveStruct((StructTypeInfo) typeInfo, context, options);
        }
      case MAP:
        {
          return new HiveMap((MapTypeInfo) typeInfo, context, options);
        }
      case UNION:
        {
          Class<? extends HiveFieldConverter> clazz = Union.class;
          if (clazz != null) {
            return clazz.getConstructor(HiveOperatorContextOptions.class).newInstance(options);
          }
        }
        break;
      default:
        throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null;
  }

  public static class Union extends HiveFieldConverter {
    public Union(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      // In ORC vectorized file reader path these functions are not called.
      // Currently we support complex types in ORC format only
      return;
    }
  }

  public abstract static class BaseComplexConverter extends HiveFieldConverter {
    private final OperatorContext context;

    protected BaseComplexConverter(OperatorContext context, HiveOperatorContextOptions options) {
      super(options);
      this.context = context;
    }

    protected void write(
        BaseWriter.ListWriter writer, TypeInfo typeInfo, ObjectInspector oi, Object value) {
      if (value == null) {
        return;
      }

      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          {
            final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
            switch (primitiveTypeInfo.getPrimitiveCategory()) {
              case BOOLEAN:
                writeBoolean(writer.bit(), ((BooleanObjectInspector) oi).get(value));
                break;
              case DOUBLE:
                writeDouble(writer.float8(), ((DoubleObjectInspector) oi).get(value));
                break;
              case FLOAT:
                writeFloat(writer.float4(), ((FloatObjectInspector) oi).get(value));
                break;
              case DECIMAL:
                writeDecimal(
                    writer.decimal(), getDecimalValue((DecimalTypeInfo) typeInfo, oi, value));
                break;
              case BYTE:
                writeInt(writer.integer(), ((ByteObjectInspector) oi).get(value));
                break;
              case INT:
                writeInt(writer.integer(), ((IntObjectInspector) oi).get(value));
                break;
              case LONG:
                writeLong(writer.bigInt(), ((LongObjectInspector) oi).get(value));
                break;
              case SHORT:
                writeInt(writer.integer(), ((ShortObjectInspector) oi).get(value));
                break;
              case BINARY:
                writeBinary(
                    writer.varBinary(), ((BinaryObjectInspector) oi).getPrimitiveJavaObject(value));
                break;
              case STRING:
                writeText(
                    writer.varChar(),
                    ((StringObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case VARCHAR:
                writeText(
                    writer.varChar(),
                    ((HiveVarcharObjectInspector) oi)
                        .getPrimitiveWritableObject(value)
                        .getTextValue());
                break;
              case TIMESTAMP:
                writeTimestamp(
                    writer.timeStampMilli(),
                    ((TimestampObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case DATE:
                writeDate(
                    writer.dateMilli(),
                    ((DateObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case CHAR:
                writeText(
                    writer.varChar(),
                    ((HiveCharObjectInspector) oi)
                        .getPrimitiveWritableObject(value)
                        .getStrippedValue());
                break;
              default:
                break;
            }
          }
          break;
        case LIST:
          writeList(writer.list(), (ListTypeInfo) typeInfo, (ListObjectInspector) oi, value);
          break;
        case MAP:
          writeMap(writer.map(false), (MapTypeInfo) typeInfo, (MapObjectInspector) oi, value);
          break;
        case STRUCT:
          writeStruct(
              writer.struct(), (StructTypeInfo) typeInfo, (StructObjectInspector) oi, value);
          break;
        default:
          break;
      }
    }

    protected void write(
        BaseWriter.StructWriter writer,
        java.lang.String name,
        TypeInfo typeInfo,
        ObjectInspector oi,
        Object value) {
      if (value == null) {
        return;
      }

      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          {
            final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
            switch (primitiveTypeInfo.getPrimitiveCategory()) {
              case BOOLEAN:
                writeBoolean(writer.bit(name), ((BooleanObjectInspector) oi).get(value));
                break;
              case DOUBLE:
                writeDouble(writer.float8(name), ((DoubleObjectInspector) oi).get(value));
                break;
              case FLOAT:
                writeFloat(writer.float4(name), ((FloatObjectInspector) oi).get(value));
                break;
              case DECIMAL:
                {
                  DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                  writeDecimal(
                      writer.decimal(name, decimalTypeInfo.scale(), decimalTypeInfo.precision()),
                      getDecimalValue(decimalTypeInfo, oi, value));
                }
                break;
              case BYTE:
                writeInt(writer.integer(name), ((ByteObjectInspector) oi).get(value));
                break;
              case INT:
                writeInt(writer.integer(name), ((IntObjectInspector) oi).get(value));
                break;
              case LONG:
                writeLong(writer.bigInt(name), ((LongObjectInspector) oi).get(value));
                break;
              case SHORT:
                writeInt(writer.integer(name), ((ShortObjectInspector) oi).get(value));
                break;
              case BINARY:
                writeBinary(
                    writer.varBinary(name),
                    ((BinaryObjectInspector) oi).getPrimitiveJavaObject(value));
                break;
              case STRING:
                writeText(
                    writer.varChar(name),
                    ((StringObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case VARCHAR:
                writeText(
                    writer.varChar(name),
                    ((HiveVarcharObjectInspector) oi)
                        .getPrimitiveWritableObject(value)
                        .getTextValue());
                break;
              case TIMESTAMP:
                writeTimestamp(
                    writer.timeStampMilli(name),
                    ((TimestampObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case DATE:
                writeDate(
                    writer.dateMilli(name),
                    ((DateObjectInspector) oi).getPrimitiveWritableObject(value));
                break;
              case CHAR:
                writeText(
                    writer.varChar(name),
                    ((HiveCharObjectInspector) oi)
                        .getPrimitiveWritableObject(value)
                        .getStrippedValue());
                break;
              default:
                break;
            }
          }
          break;
        case LIST:
          writeList(writer.list(name), (ListTypeInfo) typeInfo, (ListObjectInspector) oi, value);
          break;
        case MAP:
          writeMap(writer.map(name, false), (MapTypeInfo) typeInfo, (MapObjectInspector) oi, value);
          break;
        case STRUCT:
          writeStruct(
              writer.struct(name), (StructTypeInfo) typeInfo, (StructObjectInspector) oi, value);
          break;
        default:
          break;
      }
    }

    private OperatorContext getContext() {
      return context;
    }

    private static BigDecimal getDecimalValue(
        DecimalTypeInfo typeInfo, ObjectInspector oi, Object value) {
      BigDecimal decimal =
          ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(value).bigDecimalValue();
      return decimal.setScale(typeInfo.scale(), RoundingMode.HALF_UP);
    }

    private void writeBinary(VarBinaryWriter writer, byte[] value) {
      checkSizeLimit(value.length);
      try (ArrowBuf buf = getContext().getAllocator().buffer(value.length)) {
        buf.setBytes(0, value);
        writer.writeVarBinary(0, value.length, buf);
      }
    }

    private void writeBoolean(BitWriter writer, boolean value) {
      writer.writeBit(value ? 1 : 0);
    }

    private void writeDouble(Float8Writer writer, double value) {
      writer.writeFloat8(value);
    }

    private void writeFloat(Float4Writer writer, float value) {
      writer.writeFloat4(value);
    }

    private void writeDecimal(DecimalWriter writer, BigDecimal value) {
      writer.writeDecimal(value);
    }

    private void writeInt(IntWriter writer, int value) {
      writer.writeInt(value);
    }

    private void writeLong(BigIntWriter writer, long value) {
      writer.writeBigInt(value);
    }

    private void writeText(VarCharWriter writer, Text value) {
      checkSizeLimit(value.getLength());
      try (ArrowBuf buf = getContext().getAllocator().buffer(value.getLength())) {
        buf.setBytes(0, value.getBytes(), 0, value.getLength());
        writer.writeVarChar(0, value.getLength(), buf);
      }
    }

    private void writeTimestamp(TimeStampMilliWriter writer, TimestampWritableV2 value) {
      long seconds = value.getSeconds();
      long nanos = value.getNanos();
      long millis = seconds * 1000 + nanos / 1000 / 1000;
      writer.writeTimeStampMilli(millis);
    }

    private void writeDate(DateMilliWriter writer, DateWritableV2 value) {
      writer.writeDateMilli(value.getDays() * Date.MILLIS_PER_DAY);
    }

    protected void writeMap(
        BaseWriter.MapWriter writer, MapTypeInfo typeInfo, MapObjectInspector oi, Object value) {
      writer.startMap();
      for (Map.Entry<?, ?> e : oi.getMap(value).entrySet()) {
        writer.startEntry();
        write(
            writer.key(), typeInfo.getMapKeyTypeInfo(), oi.getMapKeyObjectInspector(), e.getKey());
        write(
            writer.value(),
            typeInfo.getMapValueTypeInfo(),
            oi.getMapValueObjectInspector(),
            e.getValue());
        writer.endEntry();
      }
      writer.endMap();
    }

    protected void writeList(
        BaseWriter.ListWriter writer,
        ListTypeInfo typeInfo,
        ListObjectInspector listOi,
        Object value) {
      writer.startList();
      for (Object o : listOi.getList(value)) {
        write(writer, typeInfo.getListElementTypeInfo(), listOi.getListElementObjectInspector(), o);
      }
      writer.endList();
    }

    protected void writeStruct(
        BaseWriter.StructWriter writer,
        StructTypeInfo typeInfo,
        StructObjectInspector oi,
        Object value) {
      writer.start();
      for (StructField field : oi.getAllStructFieldRefs()) {
        write(
            writer,
            field.getFieldName(),
            typeInfo.getStructFieldTypeInfo(field.getFieldName()),
            field.getFieldObjectInspector(),
            oi.getStructFieldData(value, field));
      }
      writer.end();
    }
  }

  public static class HiveMap extends BaseComplexConverter {
    private final MapTypeInfo typeInfo;

    public HiveMap(
        MapTypeInfo typeInfo, OperatorContext context, HiveOperatorContextOptions options) {
      super(context, options);
      this.typeInfo = typeInfo;
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      UnionMapWriter mapWriter = ((MapVector) outputVV).getWriter();
      mapWriter.setPosition(outputIndex);
      writeMap(mapWriter, typeInfo, (MapObjectInspector) oi, hiveFieldValue);
    }
  }

  public static class HiveList extends BaseComplexConverter {
    private final ListTypeInfo typeInfo;

    public HiveList(
        ListTypeInfo typeInfo, OperatorContext context, HiveOperatorContextOptions options) {
      super(context, options);
      this.typeInfo = typeInfo;
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      UnionListWriter listWriter = ((ListVector) outputVV).getWriter();
      listWriter.setPosition(outputIndex);
      writeList(listWriter, typeInfo, (ListObjectInspector) oi, hiveFieldValue);
    }
  }

  public static class HiveStruct extends BaseComplexConverter {
    private final StructTypeInfo typeInfo;

    public HiveStruct(
        StructTypeInfo typeInfo, OperatorContext context, HiveOperatorContextOptions options) {
      super(context, options);
      this.typeInfo = typeInfo;
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      StructObjectInspector structOi = (StructObjectInspector) oi;
      NullableStructWriter structWriter = ((StructVector) outputVV).getWriter();
      structWriter.setPosition(outputIndex);
      structWriter.start();
      for (Field writerField : structWriter.getField().getChildren()) {
        StructField field = structOi.getStructFieldRef(writerField.getName());
        write(
            structWriter,
            field.getFieldName(),
            typeInfo.getStructFieldTypeInfo(field.getFieldName()),
            field.getFieldObjectInspector(),
            structOi.getStructFieldData(hiveFieldValue, field));
      }
      structWriter.end();
    }
  }

  public static class Binary extends HiveFieldConverter {
    public Binary(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final byte[] value = ((BinaryObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      checkSizeLimit(value.length);
      ((VarBinaryVector) outputVV).setSafe(outputIndex, value, 0, value.length);
    }
  }

  public static class Boolean extends HiveFieldConverter {
    public Boolean(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final boolean value =
          (boolean) ((BooleanObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((BitVector) outputVV).setSafe(outputIndex, value ? 1 : 0);
    }
  }

  public static class Decimal extends HiveFieldConverter {
    private final DecimalHolder holder = new DecimalHolder();

    public Decimal(
        int precision, int scale, OperatorContext context, HiveOperatorContextOptions options) {
      super(options);
      holder.scale = scale;
      holder.precision = precision;
      holder.buffer = context.getManagedBuffer(16);
      holder.start = 0;
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      DecimalUtility.writeBigDecimalToArrowBuf(
          ((HiveDecimalObjectInspector) oi)
              .getPrimitiveJavaObject(hiveFieldValue)
              .bigDecimalValue()
              .setScale(holder.scale, RoundingMode.HALF_UP),
          holder.buffer,
          LargeMemoryUtil.capAtMaxInt(holder.start),
          DecimalVector.TYPE_WIDTH);
      ((DecimalVector) outputVV).setSafe(outputIndex, 1, 0, holder.buffer);
    }
  }

  public static class Double extends HiveFieldConverter {
    public Double(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final double value =
          (double) ((DoubleObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((Float8Vector) outputVV).setSafe(outputIndex, value);
    }
  }

  public static class Float extends HiveFieldConverter {
    public Float(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final float value =
          (float) ((FloatObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((Float4Vector) outputVV).setSafe(outputIndex, value);
    }
  }

  public static class Int extends HiveFieldConverter {
    public Int(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (int) ((IntObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((IntVector) outputVV).setSafe(outputIndex, value);
    }
  }

  // TODO (DRILL-2470)
  // Byte and short (tinyint and smallint in SQL types) are currently read as integers
  // as these smaller integer types are not fully supported in Dremio today.
  public static class Short extends HiveFieldConverter {
    public Short(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (short) ((ShortObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((IntVector) outputVV).setSafe(outputIndex, value);
    }
  }

  public static class Byte extends HiveFieldConverter {
    public Byte(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (byte) ((ByteObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((IntVector) outputVV).setSafe(outputIndex, value);
    }
  }

  public static class Long extends HiveFieldConverter {
    public Long(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final long value = (long) ((LongObjectInspector) oi).getPrimitiveJavaObject(hiveFieldValue);
      ((BigIntVector) outputVV).setSafe(outputIndex, value);
    }
  }

  public static class String extends HiveFieldConverter {
    public String(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((StringObjectInspector) oi).getPrimitiveWritableObject(hiveFieldValue);
      final int len = value.getLength();
      checkSizeLimit(len);
      final byte[] valueBytes = value.getBytes();
      ((VarCharVector) outputVV).setSafe(outputIndex, valueBytes, 0, len);
    }
  }

  public static class VarChar extends HiveFieldConverter {
    public VarChar(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value =
          ((HiveVarcharObjectInspector) oi)
              .getPrimitiveWritableObject(hiveFieldValue)
              .getTextValue();
      final int valueLen = value.getLength();
      checkSizeLimit(valueLen);
      final byte[] valueBytes = value.getBytes();
      ((VarCharVector) outputVV).setSafe(outputIndex, valueBytes, 0, valueLen);
    }
  }

  public static class Timestamp extends HiveFieldConverter {
    public Timestamp(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final TimestampWritableV2 value =
          ((TimestampObjectInspector) oi).getPrimitiveWritableObject(hiveFieldValue);
      long seconds = value.getSeconds();
      long nanos = value.getNanos();
      long millis = seconds * 1000 + nanos / 1000 / 1000;
      ((TimeStampMilliVector) outputVV).setSafe(outputIndex, millis);
    }
  }

  public static class Date extends HiveFieldConverter {

    private final boolean requiresDateConversionForJulian;

    public Date(HiveOperatorContextOptions options) {
      super(options);
      this.requiresDateConversionForJulian = HiveUtilities.requiresDateConversionForJulian(options);
    }

    public static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final DateWritableV2 writeable =
          ((DateObjectInspector) oi).getPrimitiveWritableObject(hiveFieldValue);
      ((DateMilliVector) outputVV)
          .setSafe(
              outputIndex,
              getTrueEpochInMillis(requiresDateConversionForJulian, writeable.getDays()));
    }
  }

  public static class Char extends HiveFieldConverter {
    public Char(HiveOperatorContextOptions options) {
      super(options);
    }

    @Override
    public void setSafeValue(
        ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value =
          ((HiveCharObjectInspector) oi)
              .getPrimitiveWritableObject(hiveFieldValue)
              .getStrippedValue();
      final int valueLen = value.getLength();
      checkSizeLimit(valueLen);
      final byte[] valueBytes = value.getBytes();
      ((VarCharVector) outputVV).setSafe(outputIndex, valueBytes, 0, valueLen);
    }
  }
}
