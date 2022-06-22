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
import java.io.InvalidClassException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.exceptions.RuntimeIOException;

import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Serialization/Deserialization for iceberg entities.
 */
public class IcebergSerDe {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergSerDe.class);

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

  @Deprecated
  public static byte[] serializePartitionSpecMap(Map<Integer, PartitionSpec> partitionSpecMap) {
    try {
      return serializeToByteArray(partitionSpecMap);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to serialize PartitionSpecMap");
    }
  }

  public static byte[] serializePartitionSpecAsJsonMap(Map<Integer, PartitionSpec> partitionSpecMap) {
    try {
      Map<Integer, String> specAsJsonMap = Maps.newHashMap();
      partitionSpecMap.forEach((specId, spec) -> specAsJsonMap.put(specId, PartitionSpecParser.toJson(spec)));
      return serializeToByteArray(specAsJsonMap);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to serialize PartitionSpecMap");
    }
  }

  public static Map<Integer, PartitionSpec> deserializeJsonPartitionSpecMap(Schema schema, byte[] serialized) {
    ImmutableMap.Builder<Integer, PartitionSpec> builder = ImmutableMap.builder();
    try {
      if (serialized.length > 0) {
        Map<Integer, String> specAsJsonMap = deserializeFromByteArray(serialized);
        specAsJsonMap.forEach((k,v) -> {
          builder.put(k, PartitionSpecParser.fromJson(schema, v));
        });
      }
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize PartitionSpecMap");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize PartitionSpecMap", e);
    }
  }

  public static String serializedSchemaAsJson(Schema schema) {
    return SchemaParser.toJson(schema);
  }

  public static Schema deserializedJsonAsSchema(String schema) {
    return SchemaParser.fromJson(schema);
  }

  @Deprecated
  public static Map<Integer, PartitionSpec> deserializePartitionSpecMap(byte[] serialized) {
    try {
      return deserializeFromByteArray(serialized);
    } catch (InvalidClassException e) {
      logger.debug("SerialVersionUID is mismatch for PartitionSpec Class");
      return null; //serialVersionUID is mismatch;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize PartitionSpecMap");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize PartitionSpecMap", e);
    }
  }

  public static byte[] serializePartitionSpec(PartitionSpec partitionSpec) {
    return PartitionSpecParser.toJson(partitionSpec).getBytes();
  }

  /**
   * @param schema Iceberg Table Schema
   * @param serialized PartitionSpec serialized value with byte array {Refer: serializePartitionSpec}
   * @return PartitionSpec with schema details
   */
  public static PartitionSpec deserializePartitionSpec(Schema schema, byte[] serialized) {
    return PartitionSpecParser.fromJson(schema, new String(serialized));
  }

  @Deprecated
  public static PartitionSpec deserializePartitionSpec(byte[] serialized) {
    try {
      return deserializeFromByteArray(serialized);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize PartitionSpecMap");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize PartitionSpecMap", e);
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

    SchemaConverter schemaConverter = new SchemaConverter();
    Schema icebergSchema = schemaConverter.toIcebergSchema(schema);
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
        IcebergUtils.setPartitionSpecValue(icebergPartitionData, index.get(), field, value);
        index.getAndIncrement();
      }
    });

    return icebergPartitionData;
  }

}
