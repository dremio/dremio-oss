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
package com.dremio.dac.explore.udfs;

import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

/**
 * Extract list UDFs
 */
public class ExtractList {
  public static final String LIST_LENGTH = "array_length";
  public static final String SUB_LIST = "sublist";

  public static int resolveOffset(int offset, int length) {
    if (offset < 0) {
      return length - ( - offset ) + 1;
    } else if (offset == 0) {
      return 1; // Consider offset 0 as 1 (Same as substr behavior)
    }

    return offset;
  }

  /**
   * UDF for finding the length of a list. Postgres has similar function with name <code>array_length</code>.
   */
  @FunctionTemplate(name = "array_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ArrayLength implements SimpleFunction {
    @Param private FieldReader in;
    @Output private NullableIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (!in.isSet() || in.readObject() == null) {
        out.value = 0;
        return;
      }

      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new java.lang.UnsupportedOperationException(
            String.format("'array_length' is supported only on LIST type input. Given input type : %s",
                in.getMinorType().toString()
            )
        );
      }

      out.value = ((java.util.List<?>) in.readObject()).size();
    }
  }

  // TODO (DX-3990): This method is incomplete.
  public static ArrowBuf copy(FieldReader from, FieldWriter to, ArrowBuf workBuf) {
    switch (from.getMinorType()) {
    case BIGINT:
      to.bigInt().writeBigInt(from.readLong());
      break;
    case BIT:
      to.bit().writeBit(from.readBoolean() ? 1 : 0);
      break;
    case DATEMILLI:
      DateMilliHolder dateHolder = new DateMilliHolder();
      from.read(dateHolder);
      to.dateMilli().write(dateHolder);
      break;
    case FLOAT4:
      to.float4().writeFloat4(from.readFloat());
      break;
    case FLOAT8:
      to.float8().writeFloat8(from.readDouble());
      break;
    case INT:
      to.integer().writeInt(from.readInteger());
      break;
    case INTERVALDAY:
      IntervalDayHolder intervalDayHolder = new IntervalDayHolder();
      from.read(intervalDayHolder);
      to.intervalDay().write(intervalDayHolder);
      break;
    case INTERVALYEAR:
      IntervalYearHolder intervalYearHolder = new IntervalYearHolder();
      from.read(intervalYearHolder);
      to.intervalYear().write(intervalYearHolder);
      break;
    case TIMEMILLI:
      TimeMilliHolder timeHolder = new TimeMilliHolder();
      from.read(timeHolder);
      to.timeMilli().write(timeHolder);
      break;
    case TIMESTAMPMILLI:
      TimeStampMilliHolder timeStampHolder = new TimeStampMilliHolder();
      from.read(timeStampHolder);
      to.timeStampMilli().write(timeStampHolder);
      break;
    case VARBINARY:
      byte[] binaryData = from.readByteArray();
      workBuf = workBuf.reallocIfNeeded(binaryData.length);
      workBuf.setBytes(0, binaryData);
      to.varBinary().writeVarBinary(0, binaryData.length, workBuf);
      break;
    case VARCHAR:
      Text text = from.readText();
      workBuf = workBuf.reallocIfNeeded(text.getLength());
      workBuf.setBytes(0, text.getBytes());
      to.varChar().writeVarChar(0, text.getLength(), workBuf);
      break;
    case SMALLINT:
      to.smallInt().writeSmallInt(from.readShort());
      break;
    case TINYINT:
      to.tinyInt().writeTinyInt(from.readByte());
      break;
    case LIST:
      ListWriter listWriter = to.list();
      listWriter.startList();
      while(from.next()) {
        workBuf = copy(from.reader(), (FieldWriter)listWriter, workBuf);
      }
      listWriter.endList();
      break;
    case STRUCT:
    case NULL:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case UNION:
    default:
      throw new UnsupportedOperationException(from.getMinorType().toString());
    }
    return workBuf;
  }

  /**
   * {@link OutputDerivation} for output generation of {@link SubList} UDF.
   */
  public static final class SubListOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 3);
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);

      return new CompleteType(
          ArrowType.List.INSTANCE,
          type.getChildren().get(0)
      );
    }
  }

  /**
   * Extract the given range of elements for input list.
   *
   * Parameters:
   * 1. in : input column as field reader
   * 2. offset : 1-based starting element index (inclusive) (negative if the direction is from the end)
   * 3. length : max number of elements from the start
   */
  @FunctionTemplate(name = "sublist", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = SubListOutputDerivation.class)
  public static class SubList implements SimpleFunction {
    @Param private FieldReader in;
    @Param private NullableIntHolder start; // Inclusive
    @Param private NullableIntHolder length; // Max number of elements from start
    @Output private ComplexWriter out;

    @Inject private ArrowBuf workBuf;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        out.rootAsList();
        return;
      }

      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new java.lang.UnsupportedOperationException(
            String.format("'sublist' is supported only on LIST type input. Given input type : %s",
                in.getMinorType().toString()
            )
        );
      }

      java.util.List<?> input = (java.util.List<?>) in.readObject();

      final int size = input.size();

      final int s = com.dremio.dac.explore.udfs.ExtractList.resolveOffset(start.value, size);
      final int l = length.value;
      if (size == 0 || l <= 0 || s <= 0 || s > size) {
        // return if
        // 1. the input list is empty
        // 2. 0 - elements are requested
        // 3. invalid offset (offset starts beyond the length of the list)
        out.rootAsList();
        return;
      }

      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      listWriter.startList();
      int i = 0;
      while (in.next()) {
        ++i;
        if (i >= s && i < s + l) {
          workBuf = com.dremio.dac.explore.udfs.ExtractList.copy(in.reader(),
              (org.apache.arrow.vector.complex.writer.FieldWriter)listWriter, workBuf);
        }
      }
      listWriter.endList();
    }
  }
}
