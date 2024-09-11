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
package com.dremio.sabot;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.IntervalDayWriter;
import org.apache.arrow.vector.complex.writer.IntervalYearWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * RecordSet is used to provide input & output data for testing operators via {@link
 * BaseTestOperator}. It provides a similar interface to {@link Fixtures.Table}. RecordSets are
 * different from Fixtures.Table in that schema is explicitly provided, not inferred. RecordSets
 * also have better support for complex types including arbitrarily nested lists & structs.
 *
 * <p>RecordSets can be created using the following static methods:
 *
 * <ul>
 *   <li>rs(...): create a RecordSet
 *   <li>rb(...): create a record batch for a RecordSet
 *   <li>r(...): create a record for a RecordSet
 *   <li>st(...): create a struct value tuple
 *   <li>li(...): create a list value
 * </ul>
 *
 * <p>For example:
 *
 * <p>
 *
 * <pre><code>
 *   RecordSet input = rs(INPUT_SCHEMA,
 *       rb(
 *           r(1, li("a", "list"), st("struct field 1", "struct field 2")),
 *           r(2, null, st(null, "foo")));
 * </code></pre>
 */
public class RecordSet implements Generator.Creator {

  private final BatchSchema schema;
  private final Batch[] batches;

  public RecordSet(BatchSchema schema, Batch... batches) {
    for (Batch batch : batches) {
      batch.validateRecordLength(schema.getFieldCount());
    }

    this.schema = schema;
    this.batches = batches;
  }

  /** Factory method for creating an empty RecordSet from a schema. */
  public static RecordSet rs(BatchSchema schema) {
    return new RecordSet(schema, new Batch());
  }

  /** Factory method for creating a RecordSet from a schema and one or more record batches. */
  public static RecordSet rs(BatchSchema schema, Batch... batches) {
    return new RecordSet(schema, batches);
  }

  /**
   * Factory method for creating a RecordSet from a schema and one or more records. All records will
   * be added to a single record batch.
   */
  public static RecordSet rs(BatchSchema schema, RsRecord... records) {
    return rs(schema, rb(records));
  }

  /** Factory method for creating a record batch. */
  public static Batch rb(RsRecord... records) {
    return new Batch(records);
  }

  /**
   * Factory method for creating a single record. Values in the record must be ordered the same as
   * the defined field order in the schema assigned to the RecordSet.
   */
  public static RsRecord r(Object... values) {
    return new RsRecord(values);
  }

  /**
   * Factory method for creating a tuple value that can be assigned to a struct field. Values in the
   * tuple must be ordered the same as the defined field order for the struct.
   */
  public static Tuple st(Object... values) {
    return new Tuple(values);
  }

  /** Factory method for creating a list value that can be assigned to a list field. */
  public static List<Object> li(Object... values) {
    return Arrays.asList(values);
  }

  @Override
  public Generator toGenerator(BufferAllocator allocator) {
    return new RecordSetGenerator(allocator);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public Batch[] getBatches() {
    return batches;
  }

  public int getMaxBatchSize() {
    return Arrays.stream(batches).map(b -> b.records.length).max(Integer::compareTo).orElse(0);
  }

  public int getTotalRecords() {
    return Arrays.stream(batches).map(b -> b.records.length).reduce(0, Integer::sum);
  }

  /** Represents a batch of records. */
  public static class Batch {

    final RsRecord[] records;

    public Batch(RsRecord... records) {
      this.records = Preconditions.checkNotNull(records);
    }

    public void validateRecordLength(int length) {
      for (RsRecord rec : records) {
        rec.validateRecordLength(length);
      }
    }
  }

  /** A tuple representation used for both Records and struct field values. */
  public static class Tuple {

    final Object[] values;

    public Tuple(Object... values) {
      this.values = values == null ? new Object[] {null} : values;
    }

    public Object[] getValues() {
      return values;
    }
  }

  /** Simple representation of a single record in a RecordSet. */
  public static class RsRecord extends Tuple {

    public RsRecord(Object... values) {
      super(values);
    }

    public void validateRecordLength(int length) {
      Preconditions.checkArgument(
          values.length == length,
          "Record columns do not match schema - expected %s columns: %s",
          length,
          RecordBatchValidatorDefaultImpl.expectedToString(this));
    }
  }

  /**
   * Generator implementation for RecordSet which will convert a RecordSet into one or more Arrow
   * record batches. This is used with BaseTestOperator to provide a RecordSet as input to an
   * operator under test.
   */
  private class RecordSetGenerator implements Generator {

    private final VectorContainer container;
    private final FieldWriter[] writers;
    private final BufferManager bufferManager;

    private ArrowBuf buf;
    private int currentBatch;

    public RecordSetGenerator(BufferAllocator allocator) {
      this.container = new VectorContainer(allocator);
      this.bufferManager = new BufferManagerImpl(allocator);
      this.buf = bufferManager.getManagedBuffer(256);
      this.currentBatch = 0;
      this.writers = new FieldWriter[schema.getFields().size()];
      for (int i = 0; i < writers.length; i++) {
        ValueVector vector = container.addOrGet(schema.getFields().get(i));
        this.writers[i] = vector.getMinorType().getNewFieldWriter(vector);
      }
      container.buildSchema();
    }

    @Override
    public VectorAccessible getOutput() {
      return container;
    }

    @Override
    public int next(int unused) {
      container.allocateNew();

      if (currentBatch < batches.length) {
        RsRecord[] records = batches[currentBatch++].records;

        for (int i = 0; i < records.length; i++) {
          for (int c = 0; c < writers.length; c++) {
            FieldWriter writer = writers[c];
            writer.setPosition(i);
            set(writer, schema.getColumn(c), records[i].values[c]);
          }
        }

        container.setAllCount(records.length);
        return records.length;
      }

      return 0;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(container, bufferManager);
    }

    private void set(FieldWriter writer, Field field, Object value) {
      String name = field.getName();
      Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
      switch (minorType) {
        case STRUCT:
          setStruct(writer, field, value);
          break;
        case LIST:
          setList(writer, field, value);
          break;
        case BIT:
          setBit(writer, name, value);
          break;
        case INT:
          setInt(writer, name, value);
          break;
        case BIGINT:
          setBigInt(writer, name, value);
          break;
        case FLOAT4:
          setFloat4(writer, name, value);
          break;
        case FLOAT8:
          setFloat8(writer, name, value);
          break;
        case TIMESTAMPMILLI:
          setTimeStampMilli(writer, name, value);
          break;
        case DATEMILLI:
          setDateMilli(writer, name, value);
          break;
        case TIMEMILLI:
          setTimeMilli(writer, name, value);
          break;
        case VARCHAR:
          setVarChar(writer, name, value);
          break;
        case VARBINARY:
          setVarBinary(writer, name, value);
          break;
        case INTERVALDAY:
          setIntervalDay(writer, name, value);
          break;
        case INTERVALYEAR:
          setIntervalYear(writer, name, value);
          break;
        case DECIMAL:
          setDecimal(writer, name, value);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported type %s for field %s", minorType, name));
      }
    }

    private void setBit(BitWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Boolean, "Expected boolean value for BIT field %s", name);
        writer.writeBit((Boolean) value ? 1 : 0);
      }
    }

    private void setInt(IntWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Integer, "Expected integer value for INT field %s", name);
        writer.writeInt((Integer) value);
      }
    }

    private void setBigInt(BigIntWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Long, "Expected long value for BIGINT field %s", name);
        writer.writeBigInt((Long) value);
      }
    }

    private void setFloat4(Float4Writer writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Float, "Expected float value for FLOAT4 field %s", name);
        writer.writeFloat4((Float) value);
      }
    }

    private void setFloat8(Float8Writer writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Double, "Expected double value for FLOAT8 field %s", name);
        writer.writeFloat8((Double) value);
      }
    }

    private void setTimeStampMilli(TimeStampMilliWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof LocalDateTime,
            "Expected LocalDateTime value for TIMESTAMPMILLI field %s",
            name);
        writer.writeTimeStampMilli(
            ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
      }
    }

    private void setDateMilli(DateMilliWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof LocalDate, "Expected LocalDate value for DATEMILLI field %s", name);
        writer.writeDateMilli(
            ((LocalDate) value).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());
      }
    }

    private void setTimeMilli(TimeMilliWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof LocalTime, "Expected LocalTime value for TIMEMILLI field %s", name);
        writer.writeTimeMilli(((LocalTime) value).get(ChronoField.MILLI_OF_DAY));
      }
    }

    private void setVarChar(VarCharWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof String, "Expected String value for VARCHAR field %s", name);
        byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        buf = buf.reallocIfNeeded(bytes.length);
        buf.setBytes(0, bytes);
        writer.writeVarChar(0, bytes.length, buf);
      }
    }

    private void setVarBinary(VarBinaryWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof byte[], "Expected byte[] value for VARBINARY field %s", name);
        byte[] bytes = (byte[]) value;
        buf = buf.reallocIfNeeded(bytes.length);
        buf.setBytes(0, bytes);
        writer.writeVarBinary(0, bytes.length, buf);
      }
    }

    private void setIntervalDay(IntervalDayWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Duration, "Expected Duration value for INTERVALDAY field %s", name);
        Duration duration = (Duration) value;
        int days = (int) duration.toDays();
        int millis =
            (int) ((duration.getSeconds() % 86400) * 1000) + (duration.getNano() / 1000000);
        writer.writeIntervalDay(days, millis);
      }
    }

    private void setIntervalYear(IntervalYearWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof Period, "Expected Period value for INTERVALYEAR field %s", name);
        Period period = (Period) value;
        Preconditions.checkArgument(
            period.getDays() == 0,
            "Period values with days are not supported for INTERVALYEAR field %s",
            name);
        Preconditions.checkArgument(
            period.getMonths() < 12,
            "Period values should be normalized for INTERVALYEAR field %s",
            name);
        writer.writeIntervalYear((period.getYears() * 12) + period.getMonths());
      }
    }

    private void setDecimal(DecimalWriter writer, String name, Object value) {
      if (value == null) {
        writer.writeNull();
      } else {
        Preconditions.checkArgument(
            value instanceof BigDecimal, "Expected BigDecimal value for DECIMAL field %s", name);
        writer.writeDecimal((BigDecimal) value);
      }
    }

    private void setList(BaseWriter.ListWriter listWriter, Field field, Object values) {
      if (values == null) {
        listWriter.writeNull();
      } else {
        Preconditions.checkArgument(
            values instanceof List, "Expect List value for LIST field %s", field.getName());
        List<?> list = (List<?>) values;
        listWriter.startList();

        Field child = field.getChildren().get(0);
        String name = child.getName();
        Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());
        switch (childType) {
          case STRUCT:
            BaseWriter.StructWriter childStructWriter = listWriter.struct();
            for (Object v : list) {
              setStruct(childStructWriter, child, v);
            }
            break;
          case LIST:
            BaseWriter.ListWriter childListWriter = listWriter.list();
            for (Object v : list) {
              setList(childListWriter, child, v);
            }
            break;
          case BIT:
            BitWriter bitWriter = listWriter.bit();
            for (Object v : list) {
              setBit(bitWriter, name, v);
            }
            break;
          case INT:
            IntWriter intWriter = listWriter.integer();
            for (Object v : list) {
              setInt(intWriter, name, v);
            }
            break;
          case BIGINT:
            BigIntWriter bigIntWriter = listWriter.bigInt();
            for (Object v : list) {
              setBigInt(bigIntWriter, name, v);
            }
            break;
          case FLOAT4:
            Float4Writer float4Writer = listWriter.float4();
            for (Object v : list) {
              setFloat4(float4Writer, name, v);
            }
            break;
          case FLOAT8:
            Float8Writer float8Writer = listWriter.float8();
            for (Object v : list) {
              setFloat8(float8Writer, name, v);
            }
            break;
          case TIMESTAMPMILLI:
            TimeStampMilliWriter timeStampMilliWriter = listWriter.timeStampMilli();
            for (Object v : list) {
              setTimeStampMilli(timeStampMilliWriter, name, v);
            }
            break;
          case DATEMILLI:
            DateMilliWriter dateMilliWriter = listWriter.dateMilli();
            for (Object v : list) {
              setDateMilli(dateMilliWriter, name, v);
            }
            break;
          case TIMEMILLI:
            TimeMilliWriter timeMilliWriter = listWriter.timeMilli();
            for (Object v : list) {
              setTimeMilli(timeMilliWriter, name, v);
            }
            break;
          case VARCHAR:
            VarCharWriter varCharWriter = listWriter.varChar();
            for (Object v : list) {
              setVarChar(varCharWriter, name, v);
            }
            break;
          case VARBINARY:
            VarBinaryWriter varBinaryWriter = listWriter.varBinary();
            for (Object v : list) {
              setVarBinary(varBinaryWriter, name, v);
            }
            break;
          case INTERVALDAY:
            IntervalDayWriter intervalDayWriter = listWriter.intervalDay();
            for (Object v : list) {
              setIntervalDay(intervalDayWriter, name, v);
            }
            break;
          case INTERVALYEAR:
            IntervalYearWriter intervalYearWriter = listWriter.intervalYear();
            for (Object v : list) {
              setIntervalYear(intervalYearWriter, name, v);
            }
            break;
          case DECIMAL:
            DecimalWriter decimalWriter = listWriter.decimal();
            for (Object v : list) {
              setDecimal(decimalWriter, name, v);
            }
            break;
          default:
            throw new IllegalArgumentException(
                String.format("Unsupported type %s for field %s", childType, child.getName()));
        }

        listWriter.endList();
      }
    }

    private void setStruct(BaseWriter.StructWriter structWriter, Field field, Object values) {
      if (values == null) {
        structWriter.writeNull();
      } else {
        Preconditions.checkArgument(
            values instanceof Tuple || values instanceof Object[],
            "Expect Object[] value for STRUCT field %s",
            field.getName());
        Object[] arr = values instanceof Tuple ? ((Tuple) values).values : (Object[]) values;
        List<Field> children = field.getChildren();
        Preconditions.checkArgument(
            arr.length == children.size(),
            "Expect array of size %s for STRUCT field %s",
            field.getChildren().size(),
            field.getName());
        structWriter.start();

        for (int i = 0; i < children.size(); i++) {
          Field child = children.get(i);
          String name = child.getName();
          Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());
          switch (childType) {
            case STRUCT:
              BaseWriter.StructWriter childStructWriter = structWriter.struct(name);
              setStruct(childStructWriter, child, arr[i]);
              break;
            case LIST:
              BaseWriter.ListWriter childListWriter = structWriter.list(name);
              setList(childListWriter, child, arr[i]);
              break;
            case BIT:
              setBit(structWriter.bit(name), name, arr[i]);
              break;
            case INT:
              setInt(structWriter.integer(name), name, arr[i]);
              break;
            case BIGINT:
              setBigInt(structWriter.bigInt(name), name, arr[i]);
              break;
            case FLOAT4:
              setFloat4(structWriter.float4(name), name, arr[i]);
              break;
            case FLOAT8:
              setFloat8(structWriter.float8(name), name, arr[i]);
              break;
            case TIMESTAMPMILLI:
              setTimeStampMilli(structWriter.timeStampMilli(name), name, arr[i]);
              break;
            case DATEMILLI:
              setDateMilli(structWriter.dateMilli(name), name, arr[i]);
              break;
            case TIMEMILLI:
              setTimeMilli(structWriter.timeMilli(name), name, arr[i]);
              break;
            case VARCHAR:
              setVarChar(structWriter.varChar(name), name, arr[i]);
              break;
            case VARBINARY:
              setVarBinary(structWriter.varBinary(name), name, arr[i]);
              break;
            case INTERVALDAY:
              setIntervalDay(structWriter.intervalDay(name), name, arr[i]);
              break;
            case INTERVALYEAR:
              setIntervalYear(structWriter.intervalYear(name), name, arr[i]);
              break;
            case DECIMAL:
              setDecimal(structWriter.decimal(name), name, arr[i]);
              break;
            default:
              throw new IllegalArgumentException(
                  String.format("Unsupported type %s for field %s", childType, child.getName()));
          }
        }

        structWriter.end();
      }
    }
  }
}
