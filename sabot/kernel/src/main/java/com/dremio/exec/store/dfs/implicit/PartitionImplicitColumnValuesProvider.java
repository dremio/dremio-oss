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

package com.dremio.exec.store.dfs.implicit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

public class PartitionImplicitColumnValuesProvider implements ImplicitColumnValuesProvider {

  @Override
  public List<NameValuePair<?>> getImplicitColumnValues(BufferAllocator allocator, SplitAndPartitionInfo splitInfo,
      Map<String, Field> implicitColumns, OptionResolver options) {
    List<NameValuePair<?>> nameValuePairs = new ArrayList<>();
    List<PartitionProtobuf.PartitionValue> values = splitInfo.getPartitionInfo().getValuesList();
    try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
      for (PartitionProtobuf.PartitionValue v : values) {
        Field field = implicitColumns.get(v.getColumn());
        if (field != null) {
          NameValuePair<?> nvp = getNameValuePair(allocator, field, v);
          rollbackCloseable.add(nvp);
          nameValuePairs.add(nvp);
        }
      }
      rollbackCloseable.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return nameValuePairs;
  }

  public static NameValuePair<?> getNameValuePair(
      BufferAllocator allocator, Field field, PartitionProtobuf.PartitionValue partitionValue){
    final CompleteType type = CompleteType.fromField(field);
    switch(type.toMinorType()){
      case BIGINT:
        return new ConstantColumnPopulators.BigIntNameValuePair(field.getName(), getLong(partitionValue));
      case BIT:
        return new ConstantColumnPopulators.BitNameValuePair(field.getName(), getBit(partitionValue));
      case DATE:
        return new ConstantColumnPopulators.DateMilliNameValuePair(field.getName(), getLong(partitionValue));
      case FLOAT4:
        return new ConstantColumnPopulators.Float4NameValuePair(field.getName(), getFloat(partitionValue));
      case FLOAT8:
        return new ConstantColumnPopulators.Float8NameValuePair(field.getName(), getDouble(partitionValue));
      case INT:
        return new ConstantColumnPopulators.IntNameValuePair(field.getName(), getInt(partitionValue));
      case TIME:
        return new ConstantColumnPopulators.TimeMilliNameValuePair(field.getName(), getInt(partitionValue));
      case TIMESTAMP:
        return new ConstantColumnPopulators.TimeStampMilliNameValuePair(field.getName(), getLong(partitionValue));
      case DECIMAL:
        return new TwosComplementValuePair(allocator, field, getByteArray(partitionValue));
      case VARBINARY:
        return new ConstantColumnPopulators.VarBinaryNameValuePair(field.getName(), getByteArray(partitionValue));
      case VARCHAR:
        return new ConstantColumnPopulators.VarCharNameValuePair(field.getName(), getString(partitionValue));
      default:
        throw new UnsupportedOperationException("Unable to return partition field: "  + Describer.describe(field));

    }
  }

  private static Boolean getBit(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasBitValue() ? partitionValue.getBitValue() : null;
  }

  private static byte[] getByteArray(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasBinaryValue() ? partitionValue.getBinaryValue().toByteArray() : null;
  }

  private static Double getDouble(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasDoubleValue() ? partitionValue.getDoubleValue() : null;
  }

  private static Float getFloat(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasFloatValue() ? partitionValue.getFloatValue() : null;
  }

  private static Integer getInt(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasIntValue()? partitionValue.getIntValue() : null;
  }

  private static Long getLong(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
  }

  private static String getString(PartitionProtobuf.PartitionValue partitionValue) {
    return partitionValue.hasStringValue() ? partitionValue.getStringValue() : null;
  }
}
