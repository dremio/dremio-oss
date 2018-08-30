/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.parquet2;

import static com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.getDateTimeValueFromBinary;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.joda.time.DateTimeConstants;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.options.OptionManager;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.columnreaders.DeprecatedParquetVectorizedReader;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;


abstract class ParquetGroupConverter extends GroupConverter {

  protected final List<Converter> converters;
  private final List<ListWriter> listWriters = Lists.newArrayList();
  private final OutputMutator mutator;
  protected final OptionManager options;
  //See DRILL-4203
  protected final SchemaDerivationHelper schemaHelper;

  private final GroupType schema;
  private final Collection<SchemaPath> columns;
  private final List<Field> arrowSchema;
  private final Function<String, String> childNameResolver;

  // This function assumes that the fields in the schema parameter are in the same order as the fields in the columns parameter. The
  // columns parameter may have fields that are not present in the schema, though.
  ParquetGroupConverter(
      OutputMutator mutator,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> arrowSchema,
      Function<String, String> childNameResolver,
      SchemaDerivationHelper schemaHelper) {
    this.converters = Lists.newArrayList();
    this.mutator = mutator;
    this.schema = schema;
    this.columns = columns;
    this.options = options;
    this.arrowSchema = arrowSchema;
    this.childNameResolver = childNameResolver;
    this.schemaHelper = schemaHelper;
  }

  abstract WriterProvider getWriterProvider();

  void convertChildren() {

    Iterator<SchemaPath> colIterator=columns.iterator();

    for (Type type : schema.getFields()) {
      addChildConverter(mutator, arrowSchema, colIterator, type, childNameResolver);
    }
  }

  protected void addChildConverter(OutputMutator mutator,
      List<Field> arrowSchema, Iterator<SchemaPath> colIterator, Type type, Function<String, String> childNameResolver) {
    // Match the name of the field in the schema definition to the name of the field in the query.
    String name = null;
    SchemaPath col;
    PathSegment colPath;
    PathSegment colNextChild = null;
    while (colIterator.hasNext()) {
      col = colIterator.next();
      colPath = col.getRootSegment();
      colNextChild = colPath.getChild();

      if (colPath.isNamed() && (!colPath.getNameSegment().getPath().equals("*"))) {
        name = colPath.getNameSegment().getPath();
        // We may have a field that does not exist in the schema
        if (!name.equalsIgnoreCase(type.getName())) {
          continue;
        }
      }
      break;
    }
    if (name == null) {
      name = type.getName();
    }

    final String nameForChild = childNameResolver.apply(name);
    final Converter converter = type.isPrimitive() ?
      getConverterForType(nameForChild, type.asPrimitiveType())
      : groupConverter(mutator, arrowSchema, type.asGroupType(), colNextChild, nameForChild);
    converters.add(converter);
  }

  private Converter groupConverter(OutputMutator mutator,
      List<Field> arrowSchema, GroupType groupType, PathSegment colNextChild, final String nameForChild) {
    Collection<SchemaPath> c = new ArrayList<>();

    while (colNextChild != null) {
      if (colNextChild.isNamed()) {
        break;
      }
      colNextChild = colNextChild.getChild();
    }

    if (colNextChild != null) {
      SchemaPath s = new SchemaPath(colNextChild.getNameSegment());
      c.add(s);
    }

    if (arrowSchema != null) {
      return groupConverterFromArrowSchema(nameForChild, groupType.getName(), groupType, c);
    }

    return defaultGroupConverter(mutator, groupType, nameForChild, c, null);
  }

  Converter groupConverterFromArrowSchema(String nameForChild, String fieldName, GroupType groupType, Collection<SchemaPath> c) {
    final Field arrowField = Schema.findField(arrowSchema, fieldName);
    final ArrowTypeID arrowTypeType = arrowField.getType().getTypeID();
    final List<Field> arrowChildren = arrowField.getChildren();
    if (arrowTypeType == ArrowTypeID.Union) {
      // if it's a union we will add the children directly to the parent
      return new UnionGroupConverter(mutator, getWriterProvider(), groupType, c, options, arrowChildren, nameForChild,
          schemaHelper);
    } else if (arrowTypeType == ArrowTypeID.List) {
      // make sure the parquet schema matches the arrow schema and delegate handling the logical list to defaultGroupConverter()
      Preconditions.checkState(groupType.getOriginalType() == OriginalType.LIST, "parquet schema doesn't match the arrow schema for LIST " + nameForChild);
    }

    return defaultGroupConverter(mutator, groupType, nameForChild, c, arrowChildren);
  }

  Converter defaultGroupConverter(OutputMutator mutator, GroupType groupType, final String nameForChild,
                                  Collection<SchemaPath> c, List<Field> arrowSchema) {

    if (groupType.getOriginalType() == OriginalType.LIST && LogicalListL1Converter.isSupportedSchema(groupType)) {
      return new LogicalListL1Converter(
        nameForChild,
        mutator,
        getWriterProvider(),
        groupType,
        c,
        options,
        arrowSchema,
        schemaHelper
      );
    }

    final StructWriter struct;
    if (groupType.isRepetition(REPEATED)) {
      if (arrowSchema != null) {
        //TODO assert this should never occur at this level
        // only parquet writer that writes arrowSchema doesn't write repeated fields except
        // as part of a LOGICAL LIST, thus this scenario (repeated + arrow schema present) can
        // only happen in LogicalList converter
        arrowSchema = handleRepeatedField(arrowSchema, groupType);
      }
      struct = list(nameForChild).struct();
    } else {
      struct = getWriterProvider().struct(nameForChild);
    }

    return new StructGroupConverter(mutator, struct, groupType, c, options, arrowSchema, schemaHelper);
  }

  /**
   * Validates the list type as expected.
   * @param arrowSchema current arrow schema
   * @return child schema
   */
  private List<Field> handleRepeatedField(List<Field> arrowSchema, GroupType groupType) {
    // validating that the list type is as expected
    if (arrowSchema.size() != 1 && !arrowSchema.get(0).getName().equals("$data$")) {
      UserException.dataReadError()
              .message("invalid children. Expected a single child named $data$, was actually %s for repeated type %s. ", arrowSchema, groupType);
    }
    // in the case of list, we skip over the inner type (struct = list(nameForChild).struct() bellow)
    return arrowSchema.get(0).getChildren();
  }

  protected PrimitiveConverter getConverterForType(String name, PrimitiveType type) {
    final boolean isRepeated = type.isRepetition(REPEATED);

    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        OriginalType originalType = type.getOriginalType();
        if (originalType != null) {
          switch (type.getOriginalType()) {
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalWriter writer = isRepeated ? list(name).decimal() :
              getWriterProvider().decimal(name, type.getDecimalMetadata().getScale(), type.getDecimalMetadata().getPrecision());
            return new Decimal9Converter(writer, type.getDecimalMetadata().getPrecision(), type.getDecimalMetadata().getScale(), mutator.getManagedBuffer());
          }
          case DATE: {
            DateMilliWriter writer = isRepeated ? list(name).dateMilli() : getWriterProvider().date(name);
            switch (schemaHelper.getDateCorruptionStatus()) {
            case META_SHOWS_CORRUPTION:
              return new CorruptedDateConverter(writer);
            case META_SHOWS_NO_CORRUPTION:
              return new DateConverter(writer);
            case META_UNCLEAR_TEST_VALUES:
              return new CorruptionDetectingDateConverter(writer);
            default:
              // See DRILL-4203
              throw new RuntimeException(
                String.format("Issue setting up parquet reader for date type, " +
                    "unrecognized date corruption status %s.",
                  schemaHelper.getDateCorruptionStatus()));
            }
          }
          case TIME_MILLIS: {
            TimeMilliWriter writer = isRepeated ? list(name).timeMilli() : getWriterProvider().time(name);
            return new TimeConverter(writer);
          }
          default:
            // fall back to primitive type
          }
        }
        IntWriter writer = isRepeated ? list(name).integer() : getWriterProvider().integer(name);
        return new IntConverter(writer);
      }
      case INT64: {
        OriginalType originalType = type.getOriginalType();
        if (originalType != null) {
          switch (originalType) {
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalMetadata metadata = type.getDecimalMetadata();
            DecimalWriter writer = isRepeated ? list(name).decimal() :
              getWriterProvider().decimal(name, metadata.getScale(), metadata.getPrecision());
            return new Decimal18Converter(writer, type.getDecimalMetadata().getPrecision(), type.getDecimalMetadata().getScale(), mutator.getManagedBuffer());
          }
          case TIMESTAMP_MILLIS: {
            TimeStampMilliWriter writer = isRepeated ? list(name).timeStampMilli() : getWriterProvider().timeStamp(name);
            return new TimeStampConverter(writer);
          }
          default:
            // fall back to primitive type
          }
        }
        BigIntWriter writer = isRepeated ? list(name).bigInt() : getWriterProvider().bigInt(name);
        return new BigIntConverter(writer);
      }
      case INT96: {
        // TODO: replace null with TIMESTAMP_NANOS once parquet support such type annotation.
        if (schemaHelper.readInt96AsTimeStamp()) {
          TimeStampMilliWriter writer = type.getRepetition() == Repetition.REPEATED ? getWriterProvider().list(name).timeStampMilli() : getWriterProvider().timeStamp(name);
          return new FixedBinaryToTimeStampConverter(writer);
        } else {
          VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED ? getWriterProvider().list(name).varBinary() : getWriterProvider().varBinary(name);
          return new FixedBinaryToVarbinaryConverter(writer, DeprecatedParquetVectorizedReader.getTypeLengthInBits(type.getPrimitiveTypeName()) / 8, mutator.getManagedBuffer());
        }
      }
      case FLOAT: {
        Float4Writer writer = isRepeated ? list(name).float4() : getWriterProvider().float4(name);
        return new Float4Converter(writer);
      }
      case DOUBLE: {
        Float8Writer writer = isRepeated ? list(name).float8() : getWriterProvider().float8(name);
        return new Float8Converter(writer);
      }
      case BOOLEAN: {
        BitWriter writer = isRepeated ? list(name).bit() : getWriterProvider().bit(name);
        return new BoolConverter(writer);
      }
      case BINARY: {
        OriginalType originalType = type.getOriginalType();
        if (originalType != null) {
          switch (type.getOriginalType()) {
          case UTF8: {
            VarCharWriter writer = isRepeated ? list(name).varChar() : getWriterProvider().varChar(name);
            return new VarCharConverter(writer, mutator.getManagedBuffer());
          }
          //TODO not sure if BINARY/DECIMAL is actually supported
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalMetadata metadata = type.getDecimalMetadata();
            DecimalWriter writer = isRepeated ? list(name).decimal() : getWriterProvider().decimal(name, metadata.getScale(), metadata.getPrecision());
            return new BinaryToDecimal28Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
          }
          default:
            // fall back to primitive type
          }
        }

        if (schemaHelper.isVarChar(SchemaPath.getSimplePath(name))) {
          VarCharWriter writer = isRepeated ? list(name).varChar() : getWriterProvider().varChar(name);
          return new VarCharConverter(writer, mutator.getManagedBuffer());
        }

        VarBinaryWriter writer = isRepeated ? list(name).varBinary() : getWriterProvider().varBinary(name);
        return new VarBinaryConverter(writer, mutator.getManagedBuffer());
      }
      case FIXED_LEN_BYTE_ARRAY:
        if (type.getOriginalType() == OriginalType.DECIMAL) {
          ParquetReaderUtility.checkDecimalTypeEnabled(options);
          DecimalMetadata metadata = type.getDecimalMetadata();
          DecimalWriter writer = isRepeated ? list(name).decimal() : getWriterProvider().decimal(name, metadata.getScale(), metadata.getPrecision());
            return new BinaryToDecimal28Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
        }
        if (schemaHelper.isVarChar(SchemaPath.getSimplePath(name))) {
          VarCharWriter writer = isRepeated ? list(name).varChar() : getWriterProvider().varChar(name);
          return new VarCharConverter(writer, mutator.getManagedBuffer());
        }

        VarBinaryWriter writer = isRepeated ? list(name).varBinary() : getWriterProvider().varBinary(name);
        return new FixedBinaryToVarbinaryConverter(writer, type.getTypeLength(), mutator.getManagedBuffer());
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type.getPrimitiveTypeName());
    }
  }

  private ListWriter list(String name) {
    ListWriter writer = getWriterProvider().list(name);
    //TODO should I just implement this logic in WriterProvider ?
    listWriters.add(writer);
    return writer;
  }

  @Override
  public Converter getConverter(int i) {
    return converters.get(i);
  }

  void startListWriters() {
    for (ListWriter writer : listWriters) {
      writer.startList();
    }
  }

  void endListWriters() {
    for (ListWriter writer : listWriters) {
      writer.endList();
    }
  }

  private static class IntConverter extends PrimitiveConverter {
    private IntWriter writer;
    private IntHolder holder = new IntHolder();

    private IntConverter(IntWriter writer) {
      super();
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  private static class Decimal9Converter extends PrimitiveConverter {
    private DecimalWriter writer;
    int precision;
    int scale;
    private ArrowBuf buffer;

    private Decimal9Converter(DecimalWriter writer, int precision, int scale, ArrowBuf buffer) {
      this.writer = writer;
      this.scale = scale;
      this.precision = precision;
      this.buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void addInt(int value) {
      /* set the bytes in LE format in the buffer of decimal vector */
      buffer.setInt(0, value);
      if (value < 0) {
        for (int i = 1; i < 4; i++) {
          buffer.setInt(i * 4, 0xFFFFFFFF);
        }
      } else {
        buffer.setZero(4, 12);
      }
      writer.writeDecimal(0, buffer);
    }
  }

  private static class CorruptionDetectingDateConverter extends PrimitiveConverter {
    private DateMilliWriter writer;
    private DateMilliHolder holder = new DateMilliHolder();

    private CorruptionDetectingDateConverter(DateMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      if (value > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
        holder.value = (value - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY;
      } else {
        holder.value = value * (long) DateTimeConstants.MILLIS_PER_DAY;
      }
      writer.write(holder);
    }
  }

  private static class CorruptedDateConverter extends PrimitiveConverter {
    private DateMilliWriter writer;
    private DateMilliHolder holder = new DateMilliHolder();

    private CorruptedDateConverter(DateMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = (value - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY;
      writer.write(holder);
    }
  }

  private static class DateConverter extends PrimitiveConverter {
    private DateMilliWriter writer;
    private DateMilliHolder holder = new DateMilliHolder();

    private DateConverter(DateMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value * (long) DateTimeConstants.MILLIS_PER_DAY;
      writer.writeDateMilli(holder.value);
    }
  }

  private static class TimeConverter extends PrimitiveConverter {
    private TimeMilliWriter writer;
    private TimeMilliHolder holder = new TimeMilliHolder();

    private TimeConverter(TimeMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.writeTimeMilli(holder.value);
    }
  }

  private static class BigIntConverter extends PrimitiveConverter {
    private BigIntWriter writer;
    private BigIntHolder holder = new BigIntHolder();

    private BigIntConverter(BigIntWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addLong(long value) {
      holder.value = value;
      writer.writeBigInt(holder.value);
    }
  }

  private static class TimeStampConverter extends PrimitiveConverter {
    private TimeStampMilliWriter writer;

    private TimeStampConverter(TimeStampMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addLong(long value) {
      writer.writeTimeStampMilli(value);
    }
  }

  private static class Decimal18Converter extends PrimitiveConverter {
    private DecimalWriter writer;
    private DecimalHolder holder = new DecimalHolder();
    private ArrowBuf buffer;

    private Decimal18Converter(DecimalWriter writer, int precision, int scale, ArrowBuf buffer) {
      this.writer = writer;
      holder.precision = precision;
      holder.scale = scale;
      this.buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void addLong(long value) {
      /* set the bytes in LE format in the buffer of decimal vector */
      buffer.setLong(0, value);
      if (value < 0) {
        for (int i = 2; i < 4; i++) {
          buffer.setInt(i * 4, 0xFFFFFFFF);
        }
      } else {
        buffer.setZero(8, 8);
      }
      writer.writeDecimal(0, buffer);
    }
  }

  private static class Float4Converter extends PrimitiveConverter {
    private Float4Writer writer;
    private Float4Holder holder = new Float4Holder();

    private Float4Converter(Float4Writer writer) {
      this.writer = writer;
    }

    @Override
    public void addFloat(float value) {
      holder.value = value;
      writer.writeFloat4(holder.value);
    }
  }

  private static class Float8Converter extends PrimitiveConverter {
    private Float8Writer writer;
    private Float8Holder holder = new Float8Holder();

    private Float8Converter(Float8Writer writer) {
      this.writer = writer;
    }

    @Override
    public void addDouble(double value) {
      holder.value = value;
      writer.writeFloat8(holder.value);
    }
  }

  private static class BoolConverter extends PrimitiveConverter {
    private BitWriter writer;

    private BoolConverter(BitWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBoolean(boolean value) {
      writer.writeBit(value ? 1 : 0);
    }
  }

  private static class VarBinaryConverter extends PrimitiveConverter {
    private VarBinaryWriter writer;
    private ArrowBuf buf;
    private VarBinaryHolder holder = new VarBinaryHolder();

    private VarBinaryConverter(VarBinaryWriter writer, ArrowBuf buf) {
      this.writer = writer;
      this.buf = buf;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer = buf = buf.reallocIfNeeded(value.length());
      buf.setBytes(0, value.toByteBuffer());
      holder.start = 0;
      holder.end = value.length();
      writer.writeVarBinary(holder.start, holder.end, holder.buffer);
    }
  }

  private static class VarCharConverter extends PrimitiveConverter {
    private VarCharWriter writer;
    private VarCharHolder holder = new VarCharHolder();
    private ArrowBuf buf;

    private VarCharConverter(VarCharWriter writer,  ArrowBuf buf) {
      this.writer = writer;
      this.buf = buf;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer = buf = buf.reallocIfNeeded(value.length());
      buf.setBytes(0, value.toByteBuffer());
      holder.start = 0;
      holder.end = value.length();
      writer.writeVarChar(holder.start, holder.end, holder.buffer);
    }
  }

  private static class BinaryToDecimal28Converter extends PrimitiveConverter {
    private DecimalWriter writer;
    private DecimalHolder holder = new DecimalHolder();
    private ArrowBuf buffer;

    private BinaryToDecimal28Converter(DecimalWriter writer, int precision, int scale,  ArrowBuf buffer) {
      this.writer = writer;
      this.buffer = buffer.reallocIfNeeded(16);
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      final int length = value.length();
      final byte[] bytes = value.getBytes();
      /* set the bytes in LE format in the buffer of decimal vector, we will swap
       * the bytes while writing into the vector.
       */
      writer.writeBigEndianBytesToDecimal(bytes);
    }
  }

  /**
   * Parquet currently supports a fixed binary type, which is not implemented in Dremio. For now this
   * data will be read in a s varbinary and the same length will be recorded for each value.
   */
  private static class FixedBinaryToVarbinaryConverter extends PrimitiveConverter {
    private VarBinaryWriter writer;
    private VarBinaryHolder holder = new VarBinaryHolder();

    private FixedBinaryToVarbinaryConverter(VarBinaryWriter writer, int length, ArrowBuf buf) {
      this.writer = writer;
      holder.buffer = buf.reallocIfNeeded(length);
      holder.start = 0;
      holder.end = length;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer.setBytes(0, value.toByteBuffer());
      writer.writeVarBinary(holder.start, holder.end, holder.buffer);
    }
  }

  /**
   * Parquet currently supports a fixed binary type INT96 for storing hive, impala timestamp
   * with nanoseconds precision.
   */
  public static class FixedBinaryToTimeStampConverter extends PrimitiveConverter {
    private TimeStampMilliWriter writer;
    private TimeStampMilliHolder holder = new TimeStampMilliHolder();

    public FixedBinaryToTimeStampConverter(TimeStampMilliWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBinary(Binary value) {
      holder.value = getDateTimeValueFromBinary(value);
      writer.write(holder);
    }
  }
}
