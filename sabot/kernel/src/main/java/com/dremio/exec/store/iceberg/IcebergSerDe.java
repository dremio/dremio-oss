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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;

/**
 * Serialization/Deserialization for iceberg entities.
 */
public class IcebergSerDe {

  public static byte[] serializeDataFile(DataFile dataFile) {
    try {
      return serializeToByteArray(dataFile);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to serialize DataFile");
    }
  }

  public static DataFile deserializeDataFile(byte[] serialized) {
    try {
      return (DataFile) deserializeFromByteArray(serialized);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize DataFile");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize DataFile", e);
    }
  }

  public static byte[] serializeManifestFile(ManifestFile manifestFile) {
    try {
      return serializeToByteArray(manifestFile);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to serialize ManifestFile");
    }
  }

  public static ManifestFile deserializeManifestFile(byte[] serialized) {
    try {
      return deserializeFromByteArray(serialized);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize ManifestFile");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize ManifestFile", e);
    }
  }

  public static IcebergPartitionData deserializePartitionData(byte[] serialized) {
    try {
      return deserializeFromByteArray(serialized);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize IcebergPartitionData");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize IcebergPartitionData", e);
    }
  }

  public static byte[] serializeToByteArray(Object object) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    }
  }

  public static <T> T deserializeFromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      ObjectInput in = new ObjectInputStream(bis)) {
      return (T) in.readObject();
    }
  }

  public static IcebergPartitionData partitionValueToIcebergPartition(List<PartitionProtobuf.PartitionValue> partitionValues, BatchSchema schema) {
    if(partitionValues.isEmpty()) {
      return null;
    }

    Schema icebergSchema = SchemaConverter.toIcebergSchema(schema);
    PartitionSpec.Builder partitionSpec = PartitionSpec
      .builderFor(icebergSchema);

    List<Field> schemaFields = schema.getFields();

    partitionValues.forEach(partitionValue -> {
      String columnName = partitionValue.getColumn();
      Preconditions.checkState(schemaFields.stream().anyMatch(field -> field.getName().equals(columnName)), String.format("PartitionField %s not found in batch schema", columnName));
      partitionSpec.identity(columnName);
    });

    //Name to value map
    Map<String, PartitionProtobuf.PartitionValue> nameToPartValue = partitionValues.stream().collect(Collectors.toMap(p -> p.getColumn(), p -> p));
    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.build().partitionType());

    AtomicInteger index = new AtomicInteger();

    schemaFields.forEach(field -> {
      PartitionProtobuf.PartitionValue value = nameToPartValue.get(field.getName());
      if (value != null) {
        setPartitionSpecValue(icebergPartitionData, index.get(), field, value);
        index.getAndIncrement();
      }
    });

    return icebergPartitionData;
  }

  private static void setPartitionSpecValue(IcebergPartitionData data, int position, Field field, PartitionProtobuf.PartitionValue partitionValue) {
    final CompleteType type = CompleteType.fromField(field);
    //TODO: revisit logic for DECIMAL, TIME, TIMESTAMP and DATE types once end-end flow is ready
    Object value = null;

    switch(type.toMinorType()){
      case BIGINT:
        value = partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
        data.setLong(position, (Long)value);
        break;
      case BIT:
        value =  partitionValue.hasBitValue() ? partitionValue.getBitValue() : null;
        data.setBoolean(position, (Boolean)value);
        break;
      case FLOAT4:
        value =  partitionValue.hasFloatValue() ? partitionValue.getFloatValue() : null;
        data.setFloat(position, (Float)value);
        break;
      case FLOAT8:
        value =  partitionValue.hasDoubleValue() ? partitionValue.getDoubleValue() : null;
        data.setDouble(position, (Double)value);
        break;
      case INT:
        value =  partitionValue.hasIntValue()? partitionValue.getIntValue() : null;
        data.setInteger(position, (Integer)value);
        break;
      case DECIMAL:
        value = partitionValue.hasBinaryValue() ? partitionValue.getBinaryValue().toByteArray() : null;
        if(value != null) {
          BigInteger unscaledValue = new BigInteger((byte[])value);
          data.setBigDecimal(position, new BigDecimal(unscaledValue, type.getScale()));
        }
        else {
          data.setBigDecimal(position, null);
        }
        break;
      case VARBINARY:
        value =  partitionValue.hasBinaryValue() ? partitionValue.getBinaryValue().toByteArray() : null;
        data.setBytes(position, (byte[])value);
        break;
      case VARCHAR:
        value =  partitionValue.hasStringValue() ? partitionValue.getStringValue() : null;
        data.setString(position, (String) value);
        break;
      case DATE:
        value =  partitionValue.hasIntValue() ? partitionValue.getIntValue() : null;
        if(value != null) {
          long millis = TimeUnit.MILLISECONDS.toDays((Integer)value);
          data.setInteger(position, Math.toIntExact(millis));
        }
        else {
          data.setInteger(position, null);
        }
        break;
      case TIME:
        value =  partitionValue.hasIntValue() ? partitionValue.getIntValue() : null;
        data.setInteger(position, (Integer) value);
        break;
      case TIMESTAMP:
        value = partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
        data.setLong(position, (Long)(value));
        break;
      default:
        throw new UnsupportedOperationException("Unable to return partition field: "  + Describer.describe(field));
    }
  }
}
