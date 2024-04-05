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
package com.dremio.exec.expr.fn.impl.array;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.OutputDerivation;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class ArrayFrequencyFunction {

  @FunctionTemplate(
      name = "array_frequency",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL,
      derivation = ArrayFrequencyFunction.FrequencyOutputDerivation.class)
  public static class ArrayFrequency implements SimpleFunction {
    @Param private FieldReader in;
    @Output private BaseWriter.ComplexWriter out;
    @Inject private ArrowBuf indexBuffer;
    @Inject private ArrowBuf keyBuffer;
    @Workspace private ArrayHelper.ListSorter sorter;
    @Workspace private ListFrequencyMapper mapper;

    @Override
    public void setup() {
      mapper = new com.dremio.exec.expr.fn.impl.array.ArrayFrequencyFunction.ListFrequencyMapper();
    }

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        return;
      }

      org.apache.arrow.vector.complex.impl.UnionListReader listReader =
          (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      final int size = listReader.size();
      listReader.next();
      org.apache.arrow.vector.complex.reader.FieldReader reader = listReader.reader();
      int position = reader.getPosition();
      indexBuffer = indexBuffer.reallocIfNeeded(size * 4);
      sorter =
          new com.dremio.exec.expr.fn.impl.array.ArrayHelper.ListSorter(
              indexBuffer, reader, position);
      sorter.sort(size);
      org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter mapWriter = out.rootAsMap(false);
      keyBuffer = mapper.outputFrequencyMap(size, reader, position, mapWriter, keyBuffer, sorter);
    }
  }

  public static final class FrequencyOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      CompleteType argType = args.get(0).getCompleteType();
      CompleteType keyType = argType.getOnlyChildType();
      CompleteType valueType = CompleteType.INT;
      List<Field> children =
          Arrays.asList(
              Field.notNullable("key", keyType.getType()),
              Field.nullable("value", valueType.getType()));
      return new CompleteType(
          CompleteType.MAP.getType(),
          CompleteType.struct(children).toField(MapVector.DATA_VECTOR_NAME, false));
    }
  }

  public static final class ListFrequencyMapper {

    public ListFrequencyMapper() {}

    public ArrowBuf outputFrequencyMap(
        int size,
        FieldReader reader,
        int position,
        BaseWriter.MapWriter mapWriter,
        ArrowBuf keyBuffer,
        ArrayHelper.ListSorter sorter) {
      mapWriter.startMap();
      Object trackedValue = null;
      int trackedCount = 1;
      Types.MinorType minorType = reader.getMinorType();
      for (int i = 0; i < size; i++) {
        reader.setPosition(position + sorter.mapIndex(i));
        Object currentValue = reader.readObject();
        if (Objects.equals(trackedValue, currentValue)) {
          trackedCount++;
        } else {
          if (trackedValue != null) {
            keyBuffer = writeMapEntry(minorType, mapWriter, keyBuffer, trackedValue, trackedCount);
          }
          trackedCount = 1;
          trackedValue = currentValue;
        }
      }
      if (trackedValue != null) {
        writeMapEntry(minorType, mapWriter, keyBuffer, trackedValue, trackedCount);
      }
      mapWriter.endMap();
      return keyBuffer;
    }

    private ArrowBuf writeMapEntry(
        Types.MinorType elementType,
        BaseWriter.MapWriter mapWriter,
        ArrowBuf keyBuffer,
        Object trackedValue,
        int trackedCount) {
      mapWriter.startEntry();
      keyBuffer = writeKey(mapWriter.key(), keyBuffer, elementType, trackedValue);
      mapWriter.value().integer().writeInt(trackedCount);
      mapWriter.endEntry();
      return keyBuffer;
    }

    private ArrowBuf writeKey(
        BaseWriter.MapWriter writer,
        ArrowBuf keyBuffer,
        Types.MinorType elementType,
        Object value) {
      switch (elementType) {
        case VARCHAR:
          String strValue = (value != null ? value.toString() : null);
          VarCharHolder vh = new VarCharHolder();
          byte[] b = strValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
          keyBuffer = keyBuffer.reallocIfNeeded(b.length);
          keyBuffer.setBytes(0, b);
          vh.start = 0;
          vh.end = b.length;
          vh.buffer = keyBuffer;
          writer.varChar().write(vh);
          break;
        case INT:
          writer.integer().writeInt((Integer) value);
          break;
        case BIGINT:
          writer.bigInt().writeBigInt((Long) value);
          break;
        case FLOAT4:
          writer.float4().writeFloat4((Float) value);
          break;
        case FLOAT8:
          writer.float8().writeFloat8((Double) value);
          break;
        case DECIMAL:
          writer.decimal().writeDecimal((BigDecimal) value);
          break;
        case BIT:
          writer.bit().writeBit(((Boolean) value) ? 1 : 0);
          break;
        case DATEMILLI:
          writer
              .dateMilli()
              .writeDateMilli(
                  ((LocalDateTime) value)
                      .atZone(ZoneId.of(ZoneOffset.UTC.getId()))
                      .toInstant()
                      .toEpochMilli());
          break;
        case TIMEMILLI:
          writer
              .timeMilli()
              .writeTimeMilli(
                  (int)
                      (((LocalDateTime) value)
                          .atZone(ZoneId.of(ZoneOffset.UTC.getId()))
                          .toInstant()
                          .toEpochMilli()));
          break;
        case TIMESTAMPMILLI:
          writer
              .timeStampMilli()
              .writeTimeStampMilli(
                  ((LocalDateTime) value)
                      .atZone(ZoneId.of(ZoneOffset.UTC.getId()))
                      .toInstant()
                      .toEpochMilli());
          break;
        default:
          throw UserException.functionError()
              .message("Unsupported key type: %s", elementType.toString())
              .build();
      }

      return keyBuffer;
    }
  }
}
