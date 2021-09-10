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

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import com.dremio.common.expression.CompleteType;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class IcebergPartitionData
  implements StructLike, Serializable {

  public static Class<?> getPartitionColumnClass(PartitionSpec icebergPartitionSpec, int partColPos) {
    return icebergPartitionSpec.javaClasses()[partColPos];
  }
  public static IcebergPartitionData fromStructLike(PartitionSpec icebergPartitionSpec, StructLike partitionStruct) {
    IcebergPartitionData partitionData = new IcebergPartitionData(icebergPartitionSpec.partitionType());
    for (int partColPos = 0; partColPos < partitionStruct.size(); ++partColPos) {
      Class<?>  partitionValueClass = getPartitionColumnClass(icebergPartitionSpec, partColPos);
      Object partitionValue = partitionStruct.get(partColPos, partitionValueClass);
      partitionData.set(partColPos, partitionValue);
    }
    return partitionData;
  }

  private final Types.StructType partitionType;
  private final int size;
  private final Object[] data;

  public IcebergPartitionData(Types.StructType partitionType) {
    for (Types.NestedField field : partitionType.fields()) {
      Preconditions.checkArgument(field.type().isPrimitiveType(),
        "Partitions cannot contain nested types: %s", field.type());
    }

    this.partitionType = partitionType;
    this.size = partitionType.fields().size();
    this.data = new Object[size];
  }

  /**
   * Copy constructor
   */
  private IcebergPartitionData(IcebergPartitionData toCopy) {
    this.partitionType = toCopy.partitionType;
    this.size = toCopy.size;
    this.data = copyData(toCopy.partitionType, toCopy.data);
  }

  public Type getType(int pos) {
    return partitionType.fields().get(pos).type();
  }

  public void clear() {
    Arrays.fill(data, null);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    Object value = get(pos);
    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    }

    throw new IllegalArgumentException(String.format(
      "Wrong class, %s, for object: %s",
      javaClass.getName(), String.valueOf(value)));
  }

  public Object get(int pos) {
    if (pos >= data.length) {
      return null;
    }

    if (data[pos] instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) data[pos]);
    }

    return data[pos];
  }

  public Types.StructType getPartitionType() {
    return partitionType;
  }

  @Override
  public <T> void set(int pos, T value) {
    if (value instanceof Utf8) {
      // Utf8 is not Serializable
      data[pos] = value.toString();
    } else if (value instanceof ByteBuffer) {
      // ByteBuffer is not Serializable
      ByteBuffer buffer = (ByteBuffer) value;
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      data[pos] = bytes;
    } else {
      data[pos] = value;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PartitionData{");
    for (int i = 0; i < data.length; i += 1) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(partitionType.fields().get(i).name())
        .append("=")
        .append(data[i]);
    }
    sb.append("}");
    return sb.toString();
  }

  public IcebergPartitionData copy() {
    return new IcebergPartitionData(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IcebergPartitionData that = (IcebergPartitionData) o;
    return partitionType.equals(that.partitionType) && Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    Hasher hasher = Hashing.goodFastHash(32).newHasher();
    Stream.of(data).map(Objects::hashCode).forEach(hasher::putInt);
    partitionType.fields().stream().map(Objects::hashCode).forEach(hasher::putInt);
    return hasher.hash().hashCode();
  }

  public static Object[] copyData(Types.StructType type, Object[] data) {
    List<Types.NestedField> fields = type.fields();
    Object[] copy = new Object[data.length];
    for (int i = 0; i < data.length; i += 1) {
      if (data[i] == null) {
        copy[i] = null;
      } else {
        Types.NestedField field = fields.get(i);
        switch (field.type().typeId()) {
          case STRUCT:
          case LIST:
          case MAP:
            throw new IllegalArgumentException("Unsupported type in partition data: " + type);
          case BINARY:
          case FIXED:
            byte[] buffer = (byte[]) data[i];
            copy[i] = Arrays.copyOf(buffer, buffer.length);
            break;
          case STRING:
            copy[i] = data[i].toString();
            break;
          default:
            // no need to copy the object
            copy[i] = data[i];
        }
      }
    }

    return copy;
  }

  public void setInteger(int position, Integer value) {
    set(position, value);
  }

  public void setLong(int position, Long value) {
    set(position, value);
  }

  public void setFloat(int position, Float value) {
    set(position, value);
  }

  public void setDouble(int position, Double value) {
    set(position, value);
  }

  public void setBoolean(int position, Boolean value) {
    set(position, value);
  }

  public void setString(int position, String value) {
    set(position, value);
  }

  public void setBytes(int position, byte[] value) {
    set(position, value);
  }

  public void setBigDecimal(int position, BigDecimal value) {
    set(position, value);
  }

  public void set(int position, CompleteType type, ValueVector vector, int offset) {
    if (vector.isNull(offset)) {
      set(position, null);
      return;
    }

    switch (type.toMinorType()) {
      case TINYINT:
      case UINT1:
        setInteger(position, Integer.valueOf((Byte)(vector.getObject(offset))));
        break;
      case SMALLINT:
      case UINT2:
        setInteger(position, Integer.valueOf((Short)(vector.getObject(offset))));
        break;
      case INT:
      case UINT4:
        setInteger(position, (Integer)vector.getObject(offset));
        break;
      case UINT8:
      case BIGINT:
        setLong(position, (Long)(vector.getObject(offset)));
        break;
      case FLOAT4:
        setFloat(position, ((Float)(vector.getObject(offset))));
        break;
      case FLOAT8:
        setDouble(position, ((Double)(vector.getObject(offset))));
        break;
      case BIT:
        setBoolean(position, ((Boolean)(vector.getObject(offset))));
        break;
      case VARBINARY:
        setBytes(position, ((byte[])(vector.getObject(offset))));
        break;
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL38SPARSE:
      case DECIMAL28DENSE:
      case DECIMAL38DENSE:
      case DECIMAL:
        setBigDecimal(position, ((BigDecimal)(vector.getObject(offset))));
        break;

      case DATE:
        if (vector instanceof DateMilliVector) {
          setInteger(position, Math.toIntExact(TimeUnit.MILLISECONDS.toDays(((DateMilliVector) vector).get(offset))));
        } else {
          //TODO: needs further tuning
          set(position, null);
        }
        break;
      case TIME:
      case TIMETZ:
      case TIMESTAMPTZ:
      case TIMESTAMP:
      case INTERVAL:
      case INTERVALYEAR:
      case INTERVALDAY:
        if (vector instanceof  TimeStampMilliVector) {
          setLong(position, (((TimeStampMilliVector) vector).get(offset)) * 1000);
        } else {
          //TODO: needs further tuning
          set(position, null);
        }
        break;

      case VARCHAR:
      case FIXEDCHAR:
      case FIXED16CHAR:
      case FIXEDSIZEBINARY:
      case VAR16CHAR:
        setString(position, vector.getObject(offset).toString());
        break;


      case NULL:
      case MONEY:
      case LATE:
      case STRUCT:
      case LIST:
      case GENERIC_OBJECT:
      case UNION:
        throw new IllegalArgumentException("Unsupported type in partition data: " + type.toString());

    }
  }
}
