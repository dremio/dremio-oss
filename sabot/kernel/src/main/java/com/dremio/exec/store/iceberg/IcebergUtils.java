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
package com.dremio.exec.store.iceberg;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.DremioIndexByName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTimeConstants;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;

/**
 * Class contains miscellaneous utility functions for Iceberg table operations
 */
public class IcebergUtils {

  /**
   *
   * @param schema iceberg schema
   * @return column name to integer ID mapping
   */
  public static Map<String, Integer> getIcebergColumnNameToIDMap(Schema schema) {
    Map<String, Integer> schemaNameIDMap = TypeUtil.visit(Types.StructType.of(schema.columns()), new DremioIndexByName());
    return CaseInsensitiveMap.newImmutableMap(schemaNameIDMap);
  }

  public static Object getValueFromByteBuffer(ByteBuffer byteBuffer, Field field) {
    if (byteBuffer == null) {
      return null;
    }
    Object value;
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    if (field.getType().equals(CompleteType.INT.getType())) {
      value = byteBuffer.getInt();
    } else if (field.getType().equals(CompleteType.BIGINT.getType())) {
      value = byteBuffer.getLong();
    } else if (field.getType().equals(CompleteType.FLOAT.getType())) {
      value = byteBuffer.getFloat();
    } else if (field.getType().equals(CompleteType.DOUBLE.getType())) {
      value = byteBuffer.getDouble();
    } else if (field.getType().equals(CompleteType.VARCHAR.getType())) {
      value = new String(byteBuffer.array(), StandardCharsets.UTF_8);
    } else if (field.getType().equals(CompleteType.VARBINARY.getType())) {
      value = byteBuffer;
    } else if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Decimal)) {
      value = new BigDecimal(new BigInteger(byteBuffer.array()), ((ArrowType.Decimal) field.getType()).getScale());
    } else if (field.getType().equals(CompleteType.BIT.getType())) {
      value = byteBuffer.get() != 0;
    } else if (field.getType().equals(CompleteType.DATE.getType())) {
      value = byteBuffer.getInt();
    } else if (field.getType().equals(CompleteType.TIME.getType()) || field.getType().equals(CompleteType.TIMESTAMP.getType())) {
      value = byteBuffer.getLong();
    } else {
      throw UserException.unsupportedError().message("unsupported field type: " + field).buildSilently();
    }
    return value;
  }

  public static void writeToVector(ValueVector vector, int idx, Object value) {
    if (value == null) {
      if (vector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) vector).setNull(idx);
      } else if (vector instanceof BaseVariableWidthVector) {
        ((BaseVariableWidthVector) vector).setNull(idx);
      } else {
        throw UserException.unsupportedError().message("unexpected vector type: " + vector).buildSilently();
      }
      return;
    }
    if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(idx, (Integer) value);
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(idx, (Long) value);
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(idx, (Float) value);
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(idx, (Double) value);
    } else if (vector instanceof VarCharVector) {
      ((VarCharVector) vector).setSafe(idx, new Text((String) value));
    } else if (vector instanceof VarBinaryVector) {
      ((VarBinaryVector) vector).setSafe(idx, ((ByteBuffer) value).array());
    } else if (vector instanceof DecimalVector) {
      ((DecimalVector) vector).setSafe(idx, (BigDecimal) value);
    } else if (vector instanceof BitVector) {
      ((BitVector) vector).setSafe(idx, (Boolean) value ? 1 : 0);
    } else if (vector instanceof DateMilliVector) {
      ((DateMilliVector) vector).setSafe(idx, ((Integer) value) * ((long) DateTimeConstants.MILLIS_PER_DAY));
    } else if (vector instanceof TimeStampVector) {
      ((TimeStampVector) vector).setSafe(idx, (Long) value / 1_000);
    } else if (vector instanceof TimeMilliVector) {
      ((TimeMilliVector) vector).setSafe(idx, (int) ((Long) value / 1_000));
    } else {
      throw UserException.unsupportedError().message("unsupported vector type: " + vector).buildSilently();
    }
  }
}
