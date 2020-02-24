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
package com.dremio.service.namespace;

import java.io.IOException;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;

/**
 * Utility functions that deal with conversions between protos and connector objects
 */
public final class MetadataProtoUtils {

  public static ByteString toProtobuf(BytesOutput out) {
    ByteString.Output output = ByteString.newOutput();
    try {
      out.writeTo(output);
      return output.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static PartitionProtobuf.PartitionValue toProtobuf(PartitionValue value) {
    PartitionProtobuf.PartitionValue.Builder builder = PartitionProtobuf.PartitionValue.newBuilder();
    builder.setColumn(value.getColumn());
    builder.setType(toProtobuf(value.getPartitionValueType()));

    if (!value.hasValue()) {
      return builder.build();
    }

    if (value instanceof PartitionValue.BinaryPartitionValue) {
      builder.setBinaryValue(ByteString.copyFrom(((PartitionValue.BinaryPartitionValue) value).getValue()));
    } else if (value instanceof PartitionValue.BooleanPartitionValue) {
      builder.setBitValue(((PartitionValue.BooleanPartitionValue) value).getValue());
    } else if (value instanceof PartitionValue.DoublePartitionValue) {
      builder.setDoubleValue(((PartitionValue.DoublePartitionValue) value).getValue());
    } else if (value instanceof PartitionValue.FloatPartitionValue) {
      builder.setFloatValue(((PartitionValue.FloatPartitionValue) value).getValue());
    } else if (value instanceof PartitionValue.IntPartitionValue) {
      builder.setIntValue(((PartitionValue.IntPartitionValue) value).getValue());
    } else if (value instanceof PartitionValue.LongPartitionValue) {
      builder.setLongValue(((PartitionValue.LongPartitionValue) value).getValue());
    } else if (value instanceof PartitionValue.StringPartitionValue) {
      builder.setStringValue(((PartitionValue.StringPartitionValue) value).getValue());
    } else {
      throw new IllegalArgumentException("Unknown type of partition value: " + value.getClass().getName());
    }

    return builder.build();
  }

  private static PartitionProtobuf.PartitionValueType toProtobuf(PartitionValue.PartitionValueType type) {
    switch (type) {
    case IMPLICIT:
      return PartitionProtobuf.PartitionValueType.IMPLICIT;
    case INVISIBLE:
      return PartitionProtobuf.PartitionValueType.INVISIBLE;
    case VISIBLE:
      return PartitionProtobuf.PartitionValueType.VISIBLE;
    default:
      throw new IllegalArgumentException("Unknown type of partition: " + type.getClass().getName());
    }
  }

  public static PartitionProtobuf.Affinity toProtobuf(DatasetSplitAffinity affinity) {
    return PartitionProtobuf.Affinity.newBuilder()
        .setFactor(affinity.getFactor())
        .setHost(affinity.getHost())
        .build();
  }

  public static PartitionProtobuf.DatasetSplit toProtobuf(DatasetSplit split) {
    return PartitionProtobuf.DatasetSplit.newBuilder()
        .addAllAffinities(FluentIterable
            .from(split.getAffinities())
            .transform(MetadataProtoUtils::toProtobuf))
        .setSize(split.getSizeInBytes())
        .setSplitExtendedProperty(MetadataProtoUtils.toProtobuf(split.getExtraInfo()))
        .build();
  }

  // prevent instantiation
  private MetadataProtoUtils() {
  }
}
