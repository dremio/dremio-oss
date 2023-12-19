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

import static com.dremio.exec.store.iceberg.IcebergIncrementalRefreshJoinKeyTableFunction.verifyPartitionSupportedAndTransform;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.buildPartitionSpec;
import static com.dremio.exec.store.iceberg.ManifestEntryProcessorHelper.writePartitionValue;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Test class for IncrementalRefreshJoinKeyTableFunction
 */
public class TestIncrementalRefreshJoinKeyTableFunction {

  public static BatchSchema batchSchema;
  Schema icebergSchema;
  @Before
  public void setup() throws Exception {


    icebergSchema = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "decimal0_part", Types.DecimalType.of(10, 0)),
      optional(3, "decimal9_part", Types.DecimalType.of(6, 2)),
      optional(4, "decimal18_part", Types.DecimalType.of(15, 5)),
      optional(5, "decimal28_part", Types.DecimalType.of(23, 1)),
      optional(6, "decimal38_part", Types.DecimalType.of(30, 3)),
      optional(7, "double_part", Types.DoubleType.get()),
      optional(8, "float_part", Types.FloatType.get()),
      optional(9, "int_part", Types.IntegerType.get()),
      optional(10, "bigint_part", Types.LongType.get()),
      optional(11, "string_part", Types.StringType.get()),
      optional(12, "varchar_part", Types.StringType.get()),
      optional(13, "timestamp_part", Types.TimestampType.withZone()),
      optional(14, "date_part", Types.DateType.get()),
      optional(15, "char_part", Types.StringType.get())
    );
    final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    batchSchema = schemaConverter.fromIceberg(icebergSchema);
  }


  @Test
  public void verifyPartitionSupportedAndTransformIdentityIdentity() {

    //if both transforms are identity on the same column everything works for all data types
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1,
       1
    );

    verifyPartitionSupportedAndTransformHelper("decimal0_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1.0,
      1.0
    );

    verifyPartitionSupportedAndTransformHelper("double_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1.0,
      1.0
    );

    verifyPartitionSupportedAndTransformHelper("float_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1.0,
      1.0
    );


    verifyPartitionSupportedAndTransformHelper("int_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      2,
      2
    );

    verifyPartitionSupportedAndTransformHelper("bigint_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1.0,
      1.0
    );

    verifyPartitionSupportedAndTransformHelper("string_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      "test",
      "test"
    );

    verifyPartitionSupportedAndTransformHelper("timestamp_part",
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      PartitionProtobuf.IcebergTransformType.IDENTITY,
      null,
      1000,
      1000
    );
  }

    @Test
    public void verifyPartitionSupportedAndTransformIdentityMonth() {

      //month only works on the correct datatype
      verifyPartitionSupportedAndTransformHelper("id",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        1,
        null //fails
      );

      verifyPartitionSupportedAndTransformHelper("decimal0_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        1.0,
        null //fails
      );

      verifyPartitionSupportedAndTransformHelper("double_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        1.0,
        null //fails
      );

      verifyPartitionSupportedAndTransformHelper("float_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        1.0,
        null //fails
      );


      verifyPartitionSupportedAndTransformHelper("int_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        2,
        null //fails
      );

      verifyPartitionSupportedAndTransformHelper("bigint_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        1.0,
        null //fails
      );

      verifyPartitionSupportedAndTransformHelper("string_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        "test",
        null //fails
      );

    final String timestampStr = "2023-06-12T18:59:26.887";
    verifyPartitionSupportedAndTransformHelper("timestamp_part",
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        DateTimeUtil.isoTimestampToMicros(timestampStr),
        DateTimeUtil.microsToMonths(DateTimeUtil.isoTimestampToMicros(timestampStr)) //works with timestamp
      );


      verifyPartitionSupportedAndTransformHelper("date_part",
          PartitionProtobuf.IcebergTransformType.MONTH,
          null,
          PartitionProtobuf.IcebergTransformType.IDENTITY,
          null,
          DateTimeUtil.microsToMonths((DateTimeUtil.isoTimestampToMicros(timestampStr))),
          null //doesn't work
        );

      //now lets try the reverse, top is IDENTITY, bottom transform is month
      verifyPartitionSupportedAndTransformHelper("timestamp_part",
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        DateTimeUtil.microsToMonths((DateTimeUtil.isoTimestampToMicros(timestampStr))),
        null //doesn't work
      );


    }
  @Test
  public void verifyPartitionSupportedAndTransformDayYear() {

    //year transform only works on the correct datatype
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      1,
      null //fails
    );

    verifyPartitionSupportedAndTransformHelper("decimal0_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      1.0,
      null //fails
    );

    verifyPartitionSupportedAndTransformHelper("double_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      1.0,
      null //fails
    );

    verifyPartitionSupportedAndTransformHelper("float_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      1.0,
      null //fails
    );


    verifyPartitionSupportedAndTransformHelper("int_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      2,
      null //fails
    );

    verifyPartitionSupportedAndTransformHelper("bigint_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      1.0,
      null //fails
    );

    verifyPartitionSupportedAndTransformHelper("string_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      "test",
      null //fails
    );

    final String timestampStr = "2023-06-12T18:59:26.887";
    verifyPartitionSupportedAndTransformHelper("timestamp_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      DateTimeUtil.microsToDays(DateTimeUtil.isoTimestampToMicros(timestampStr)),
      DateTimeUtil.microsToYears(DateTimeUtil.isoTimestampToMicros(timestampStr)) //works with timestamp
    );


    verifyPartitionSupportedAndTransformHelper("date_part",
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      DateTimeUtil.microsToDays(DateTimeUtil.isoTimestampToMicros(timestampStr)),
      DateTimeUtil.microsToYears(DateTimeUtil.isoTimestampToMicros(timestampStr)) // works with date
    );

    //now lets try the reverse, top is DAY, bottom transform is YEAR
    verifyPartitionSupportedAndTransformHelper("timestamp_part",
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      DateTimeUtil.microsToYears(DateTimeUtil.isoTimestampToMicros(timestampStr)) ,
      null //doesn't work
    );

    verifyPartitionSupportedAndTransformHelper("date_part",
      PartitionProtobuf.IcebergTransformType.YEAR,
      null,
      PartitionProtobuf.IcebergTransformType.DAY,
      null,
      DateTimeUtil.microsToYears(DateTimeUtil.isoTimestampToMicros(timestampStr)),
      null// doesn't work
    );


  }

  @Test
  public void verifyTruncateString() {

    final String sampleString = "This is a test string";
    int truncateLengthBottom = 5;
    int truncateLengthTop = 5;

    //verify the same length
    verifyPartitionSupportedAndTransformHelper("string_part",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleString,
      sampleString.substring(0,truncateLengthTop)
    );

    //verify longer top
    truncateLengthTop = 6;
    verifyPartitionSupportedAndTransformHelper("string_part",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleString,
      null //fails because bottom truncate length must be greater than or equal to the top one
    );

    //verify longer bottom
    truncateLengthBottom = 7;
    verifyPartitionSupportedAndTransformHelper("string_part",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleString,
      sampleString.substring(0,truncateLengthTop)
    );
  }

  @Test
  public void verifyTruncateInteger() {

    final int sampleValue=  343432;
    int truncateLengthBottom = 5;
    int truncateLengthTop = 5;

    //verify the same length
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleValue,
      (sampleValue /truncateLengthTop) * truncateLengthTop
    );

    //verify truncateLengthTop % truncateLengthBottom = 0
    truncateLengthTop = 10;
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleValue,
      (sampleValue /truncateLengthTop) * truncateLengthTop
    );

    //verify truncateLengthBottom % truncateLengthTop = 0
    truncateLengthBottom = 20;
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleValue,
      null //fails
    );

    //verify random lengths
    truncateLengthBottom = 11;
    verifyPartitionSupportedAndTransformHelper("id",
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthBottom,
      PartitionProtobuf.IcebergTransformType.TRUNCATE,
      truncateLengthTop,
      sampleValue,
      null
    );
  }

  public void verifyPartitionSupportedAndTransformHelper(final String columnName,
                                                         final PartitionProtobuf.IcebergTransformType sourceTransformType,
                                                         final Integer sourceTransformArgument,
                                                         final PartitionProtobuf.IcebergTransformType targetTransformType,
                                                         final Integer targetTransformArgument,
                                                         final Object sourceValue,
                                                         final Object expectedValue){


    try {
      final PartitionSpec targetPartitionSpec = buildPartitionSpec(icebergSchema, columnName, targetTransformType, targetTransformArgument);
      final PartitionSpec sourcePartitionSpec = buildPartitionSpec(icebergSchema, columnName, sourceTransformType, sourceTransformArgument);
      final PartitionField targetPartitionField = targetPartitionSpec.fields().get(0);
      final PartitionProtobuf.NormalizedPartitionInfo normalizedPartitionInfo = buildPartitionInfo(columnName, sourceTransformType, sourceValue);
      final Object result = verifyPartitionSupportedAndTransform(targetPartitionField, targetPartitionSpec, normalizedPartitionInfo, sourcePartitionSpec);
      assertEquals(expectedValue, result);
    } catch(final Exception e){
      if (expectedValue != null){ //null means it is not supposed to work and throw an exception
        throw e;
      }
    }

  }

  public PartitionProtobuf.NormalizedPartitionInfo buildPartitionInfo(final String columnName,
                                                                      final PartitionProtobuf.IcebergTransformType sourceTransformType,
                                                                      final Object value){

    final PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
    partitionValueBuilder.setColumn(columnName);
    writePartitionValue(partitionValueBuilder,
      value,
      batchSchema.findField(columnName),
      false,
      PartitionProtobuf.IcebergTransformType.IDENTITY.equals(sourceTransformType));

    return PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
      .addIcebergValues(partitionValueBuilder.build())
      .build();
  }


}
