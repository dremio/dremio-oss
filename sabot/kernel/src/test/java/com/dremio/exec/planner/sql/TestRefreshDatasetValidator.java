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
package com.dremio.exec.planner.sql;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.DateTimes;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.planner.sql.handlers.refresh.RefreshDatasetValidator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.implicit.DecimalTools;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/** Unit test for refresh dataset validator */
public class TestRefreshDatasetValidator {
  private static final String INTEGER_COL = "integerCol";
  private static final String FLOAT_COL = "floatCol";
  private static final String DOUBLE_COL = "doubleCol";
  private static final String BIT_COL = "bitCol";
  private static final String VARCHAR_COL = "varCharCol";
  private static final String BIGINT_COL = "bigIntCol";
  private static final String DECIMAL_COL = "decimalCol";
  private static final String VARBINARY_COL = "varBinaryCol";
  private static final String TIMESTAMP_COL = "timeStampCol";
  private static final String DATE_COL = "dateCol";

  private Map<String, String> partitionKVMap;
  private BatchSchema batchSchema;

  @Before
  public void setup() {
    batchSchema =
        BatchSchema.of(
            /* 0 */ CompleteType.INT.toField(INTEGER_COL),
            /* 1 */ CompleteType.FLOAT.toField(FLOAT_COL),
            /* 2 */ CompleteType.DOUBLE.toField(DOUBLE_COL),
            /* 3 */ CompleteType.BIT.toField(BIT_COL),
            /* 4 */ CompleteType.VARCHAR.toField(VARCHAR_COL),
            /* 5 */ CompleteType.BIGINT.toField(BIGINT_COL),
            /* 6 */ CompleteType.fromDecimalPrecisionScale(0, 4).toField(DECIMAL_COL),
            /* 7 */ CompleteType.VARBINARY.toField(VARBINARY_COL),
            /* 8 */ CompleteType.TIMESTAMP.toField(TIMESTAMP_COL),
            /* 9 */ CompleteType.DATE.toField(DATE_COL));

    partitionKVMap = new HashMap<>();
    partitionKVMap.put(INTEGER_COL, "20");
    partitionKVMap.put(FLOAT_COL, "20.22");
    partitionKVMap.put(DOUBLE_COL, "40.2342");
    partitionKVMap.put(BIT_COL, "true");
    partitionKVMap.put(VARCHAR_COL, "varCharValue");
    partitionKVMap.put(BIGINT_COL, "200000000");
    partitionKVMap.put(DECIMAL_COL, "-12345.6789");
    partitionKVMap.put(VARBINARY_COL, "varBinaryString");
    partitionKVMap.put(TIMESTAMP_COL, "2020-01-01 00:00:00"); // In format: YYYY-MM-DD HH24:MI:SS
    partitionKVMap.put(DATE_COL, "2020-01-01"); // In format: YYYY-MM-DD
  }

  @Test
  public void testPartitionValuesConverter() {
    List<PartitionValue> partitionValues =
        RefreshDatasetValidator.convertToPartitionValue(partitionKVMap, batchSchema, false);

    assertEquals(20, getPartitionValue(partitionValues.get(0)));
    assertEquals(20.22f, getPartitionValue(partitionValues.get(1)));
    assertEquals(40.2342, getPartitionValue(partitionValues.get(2)));
    assertEquals(true, getPartitionValue(partitionValues.get(3)));
    assertEquals("varCharValue", getPartitionValue(partitionValues.get(4)));
    assertEquals(200000000L, getPartitionValue(partitionValues.get(5)));

    ByteBuffer b = (ByteBuffer) getPartitionValue(partitionValues.get(6));
    assertEquals(
        DecimalTools.toBinary(
            DecimalUtils.convertBigDecimalToArrowByteArray(new BigDecimal("-12345.6789"))),
        DecimalTools.toBinary(DecimalUtils.convertDecimalBytesToArrowByteArray(b.array())));

    b = (ByteBuffer) getPartitionValue(partitionValues.get(7));
    assertEquals("varBinaryString", UTF_8.decode(b).toString());

    assertEquals(
        "2020-01-01 00:00:00",
        DateTimes.toJdbcTimestampFromMillisInUtc((Long) getPartitionValue(partitionValues.get(8))));
    assertEquals(
        "2020-01-01",
        DateTimes.toJdbcDateFromMillisInUtc((Long) getPartitionValue(partitionValues.get(9))));
  }

  @Test
  public void testNegativePartitionValuesConverterForDate() {
    partitionKVMap = new HashMap<>();
    partitionKVMap.put(DATE_COL, "2020-20-10");
    try {
      RefreshDatasetValidator.convertToPartitionValue(partitionKVMap, batchSchema, false);
      fail("Any date format other than yyyy-mm-dd should have failed");
    } catch (DateTimeParseException ex) {
      assertTrue(ex.getMessage().contains("Text '2020-20-10' could not be parsed"));
    }
  }

  @Test
  public void testNegativePartitionValuesConverterForTimeStamp() {
    partitionKVMap = new HashMap<>();
    partitionKVMap.put(TIMESTAMP_COL, "2020-20-10 12:34:56");
    try {
      RefreshDatasetValidator.convertToPartitionValue(partitionKVMap, batchSchema, false);
      fail("Any timestamp format other than yyyy-mm-dd hh:mm:ss should have failed");
    } catch (DateTimeParseException ex) {
      assertTrue(ex.getMessage().contains("Text '2020-20-10 12:34:56' could not be parsed"));
    }
  }

  @Test
  public void testPartitionValuesSerde() {
    List<PartitionValue> partitionValues =
        RefreshDatasetValidator.convertToPartitionValue(partitionKVMap, batchSchema, false);

    // Convert to partitionProtos
    List<PartitionProtobuf.PartitionValue> partitionValueProtos =
        partitionValues.stream().map(MetadataProtoUtils::toProtobuf).collect(Collectors.toList());

    // Convert to IcebergPartitionData
    IcebergPartitionData partitionData =
        IcebergSerDe.partitionValueToIcebergPartition(partitionValueProtos, batchSchema);

    assertNotNull("Partition values should not be empty", partitionData);
    assertEquals(
        "struct<"
            + "1000: integerCol: optional int, "
            + "1001: floatCol: optional float, "
            + "1002: doubleCol: optional double, "
            + "1003: bitCol: optional boolean, "
            + "1004: varCharCol: optional string, "
            + "1005: bigIntCol: optional long, "
            + "1006: decimalCol: optional decimal(0, 4), "
            + "1007: varBinaryCol: optional binary, "
            + "1008: timeStampCol: optional timestamptz, "
            + "1009: dateCol: optional date"
            + ">",
        partitionData.getPartitionType().toString());
  }

  private Object getPartitionValue(PartitionValue partitionValue) {
    switch (partitionValue.getColumn()) {
      case INTEGER_COL:
        return ((PartitionValue.IntPartitionValue) partitionValue).getValue();
      case FLOAT_COL:
        return ((PartitionValue.FloatPartitionValue) partitionValue).getValue();
      case DOUBLE_COL:
        return ((PartitionValue.DoublePartitionValue) partitionValue).getValue();
      case BIT_COL:
        return ((PartitionValue.BooleanPartitionValue) partitionValue).getValue();
      case VARCHAR_COL:
        return ((PartitionValue.StringPartitionValue) partitionValue).getValue();
      case BIGINT_COL:
      case TIMESTAMP_COL:
      case DATE_COL:
        return ((PartitionValue.LongPartitionValue) partitionValue).getValue();
      case DECIMAL_COL:
      case VARBINARY_COL:
        return ((PartitionValue.BinaryPartitionValue) partitionValue).getValue();
      default:
        throw new RuntimeException(String.format("Unknown column %s", partitionValue.getColumn()));
    }
  }
}
