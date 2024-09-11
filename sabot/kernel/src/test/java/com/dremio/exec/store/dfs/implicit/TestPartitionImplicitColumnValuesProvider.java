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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecTest;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

public class TestPartitionImplicitColumnValuesProvider extends ExecTest {

  protected static final OptionResolver OPTION_RESOLVER =
      OptionResolverSpecBuilder.build(new OptionResolverSpec());
  protected static final String IMPLICIT_1 = "implicit_1";
  protected static final String IMPLICIT_2 = "implicit_2";
  protected static final String IMPLICIT_3 = "implicit_3";

  @Test
  public void testBitPartition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.BIT.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, true);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testIntPartition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.INT.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, 100);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testBigintPartition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.BIGINT.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, 100L);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testFloat4Partition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.FLOAT4.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, 100.0f);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testFloat8Partition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.FLOAT8.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, 100.0);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testBinaryPartition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.VARBINARY.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, new byte[] {0x00, 0x01});

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testStringPartition() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.VARCHAR.getType()));
    Map<String, Object> allImplicitValues = ImmutableMap.of(IMPLICIT_1, "100");

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testMultiplePartitions() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.VARCHAR.getType()),
            IMPLICIT_2, Field.nullable(IMPLICIT_2, Types.MinorType.INT.getType()),
            IMPLICIT_3, Field.nullable(IMPLICIT_3, Types.MinorType.INT.getType()));
    Map<String, Object> allImplicitValues =
        ImmutableMap.of(
            IMPLICIT_1, "100",
            IMPLICIT_2, 200,
            IMPLICIT_3, 300);

    validate(implicitColumns, allImplicitValues, allImplicitValues);
  }

  @Test
  public void testPartialSelectWithMultiplePartitions() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            IMPLICIT_1, Field.nullable(IMPLICIT_1, Types.MinorType.VARCHAR.getType()),
            IMPLICIT_3, Field.nullable(IMPLICIT_3, Types.MinorType.INT.getType()));
    Map<String, Object> allImplicitValues =
        ImmutableMap.of(
            IMPLICIT_1, "100",
            IMPLICIT_2, 200,
            IMPLICIT_3, 300);
    Map<String, Object> expectedImplicitValues =
        ImmutableMap.of(IMPLICIT_1, "100", IMPLICIT_3, 300);

    validate(implicitColumns, allImplicitValues, expectedImplicitValues);
  }

  protected ImplicitColumnValuesProvider getProvider() {
    return new PartitionImplicitColumnValuesProvider();
  }

  protected void validate(
      Map<String, Field> implicitColumns,
      Map<String, Object> allImplicitValues,
      Map<String, Object> expectedImplicitValues) {
    SplitAndPartitionInfo partitionInfo = createSplitAndPartitionInfo(allImplicitValues);
    List<NameValuePair<?>> pairs = null;
    try {
      pairs =
          getProvider()
              .getImplicitColumnValues(
                  getTestAllocator(), partitionInfo, implicitColumns, OPTION_RESOLVER);
      Map<String, Object> actualValues =
          pairs.stream().collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
      assertThat(actualValues).containsExactlyInAnyOrderEntriesOf(expectedImplicitValues);
    } finally {
      AutoCloseables.close(RuntimeException.class, pairs);
    }
  }

  protected SplitAndPartitionInfo createSplitAndPartitionInfo(Map<String, Object> values) {
    List<PartitionProtobuf.PartitionValue> partitionValues =
        values.entrySet().stream()
            .map(entry -> createPartitionValue(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
            .addAllValues(partitionValues)
            .build();
    return new SplitAndPartitionInfo(partitionInfo, null);
  }

  protected PartitionProtobuf.PartitionValue createPartitionValue(String column, Object value) {
    PartitionProtobuf.PartitionValue.Builder builder =
        PartitionProtobuf.PartitionValue.newBuilder();
    builder.setColumn(column);
    if (value instanceof Boolean) {
      builder.setBitValue((boolean) value);
    } else if (value instanceof Integer) {
      builder.setIntValue((int) value);
    } else if (value instanceof Long) {
      builder.setLongValue((long) value);
    } else if (value instanceof Float) {
      builder.setFloatValue((float) value);
    } else if (value instanceof Double) {
      builder.setDoubleValue((double) value);
    } else if (value instanceof byte[]) {
      builder.setBinaryValue(ByteString.copyFrom((byte[]) value));
    } else if (value instanceof String) {
      builder.setStringValue((String) value);
    } else {
      throw new IllegalArgumentException("Unsupported value type");
    }

    return builder.build();
  }
}
