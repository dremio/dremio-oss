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

import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.buildReflectionPartitionColumn2TransformInfoMap;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.eliminateInvalidPartitionCandidates;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.eliminateSimilarPartitionCandidates;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.generateResolvedReflectionTransformInfo;
import static org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestIncrementalReflectionByPartitionUtils {

  private final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private SqlOperatorTable operatorTable;
  private Schema icebergSchema;

  private TableMetadata tableMetadata1;
  private TableMetadata tableMetadata2;

  @Before
  public void setup() throws Exception {
    icebergSchema =
        new Schema(
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
            optional(15, "char_part", Types.StringType.get()));
    final FunctionImplementationRegistry functionImplementationRegistry =
        FunctionImplementationRegistry.create(
            ExecTest.DEFAULT_SABOT_CONFIG, ExecTest.CLASSPATH_SCAN_RESULT);
    operatorTable = DremioCompositeSqlOperatorTable.create(functionImplementationRegistry);
    tableMetadata1 = createMockTableMetadata("Table1");
    tableMetadata2 = createMockTableMetadata("Table2");
  }

  private TableMetadata createMockTableMetadata(String tableName) {
    NamespaceKey namespaceKey = new NamespaceKey(ImmutableList.of(tableName));
    TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
    when(tableMetadata.getName()).thenReturn(namespaceKey);
    DatasetConfig datasetConfig = new DatasetConfig(); // cannot use mock as class is final
    datasetConfig.setId(new EntityId(tableName + "Id"));
    when(tableMetadata.getDatasetConfig()).thenReturn(datasetConfig);
    return tableMetadata;
  }

  @Test
  public void testBuildReflectionPartitionColumn2TransformInfoMapEmpty() {
    final PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergSchema).build();
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo>
        reflectionPartitionColumn2TransformInfo =
            buildReflectionPartitionColumn2TransformInfoMap(partitionSpec);

    assertEquals(0, reflectionPartitionColumn2TransformInfo.size());
  }

  @Test
  public void testBuildReflectionPartitionColumn2TransformInfoMap() {
    final PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .identity("id")
            .truncate("int_part", 10)
            .hour("timestamp_part")
            .month("date_part")
            .build();
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo>
        reflectionPartitionColumn2TransformInfo =
            buildReflectionPartitionColumn2TransformInfoMap(partitionSpec);

    assertEquals(4, reflectionPartitionColumn2TransformInfo.size());

    final IncrementalReflectionByPartitionUtils.TransformInfo idTransformInfo =
        reflectionPartitionColumn2TransformInfo.get("id");
    assertEquals(
        PartitionProtobuf.IcebergTransformType.IDENTITY, idTransformInfo.getTransformType());
    assertNull(idTransformInfo.getBaseColumnName());
    assertNull(idTransformInfo.getArgument());

    final IncrementalReflectionByPartitionUtils.TransformInfo intPartTransformInfo =
        reflectionPartitionColumn2TransformInfo.get("int_part");
    assertEquals(
        PartitionProtobuf.IcebergTransformType.TRUNCATE, intPartTransformInfo.getTransformType());
    assertNull(intPartTransformInfo.getBaseColumnName());
    assertEquals((Integer) 10, intPartTransformInfo.getArgument());

    final IncrementalReflectionByPartitionUtils.TransformInfo timestampPartTransformInfo =
        reflectionPartitionColumn2TransformInfo.get("timestamp_part");
    assertEquals(
        PartitionProtobuf.IcebergTransformType.HOUR, timestampPartTransformInfo.getTransformType());
    assertNull(timestampPartTransformInfo.getBaseColumnName());
    assertNull(timestampPartTransformInfo.getArgument());

    final IncrementalReflectionByPartitionUtils.TransformInfo datePartTransformInfo =
        reflectionPartitionColumn2TransformInfo.get("date_part");
    assertEquals(
        PartitionProtobuf.IcebergTransformType.MONTH, datePartTransformInfo.getTransformType());
    assertNull(datePartTransformInfo.getBaseColumnName());
    assertNull(datePartTransformInfo.getArgument());
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesSameSpec() {
    final PartitionSpec partitionSpecReflection =
        PartitionSpec.builderFor(icebergSchema)
            .identity("id")
            .truncate("int_part", 10)
            .hour("timestamp_part")
            .month("date_part")
            .build();

    final PartitionSpec partitionSpecBase =
        PartitionSpec.builderFor(icebergSchema)
            .identity("id")
            .truncate("int_part", 10)
            .hour("timestamp_part")
            .month("date_part")
            .build();

    testPartitionCandidatesHelper(partitionSpecReflection, partitionSpecBase, 4);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesEmptyBase() {
    final PartitionSpec partitionSpecReflection =
        PartitionSpec.builderFor(icebergSchema)
            .identity("id")
            .truncate("int_part", 10)
            .hour("timestamp_part")
            .month("date_part")
            .build();

    final PartitionSpec partitionSpecBase = PartitionSpec.builderFor(icebergSchema).build();

    testPartitionCandidatesHelper(partitionSpecReflection, partitionSpecBase, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesEmptyReflection() {
    final PartitionSpec partitionSpecBase =
        PartitionSpec.builderFor(icebergSchema)
            .identity("id")
            .truncate("int_part", 10)
            .hour("timestamp_part")
            .month("date_part")
            .build();

    final PartitionSpec partitionSpecReflection = PartitionSpec.builderFor(icebergSchema).build();

    testPartitionCandidatesHelper(partitionSpecReflection, partitionSpecBase, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesTruncateOnIntSame() {
    testTruncateHelper("int_part", 10, 10, 1);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesTruncateOnIntLessThan() {
    testTruncateHelper("int_part", 10, 9, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesTruncateOnIntMoreThan() {
    testTruncateHelper("int_part", 9, 10, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesTruncateOnIntRModBeqZero() {
    testTruncateHelper("int_part", 10, 5, 1);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesTruncateOnIntBModReqZero() {
    testTruncateHelper("int_part", 5, 10, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesStringSame() {
    testTruncateHelper("string_part", 6, 6, 1);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesStringRltB() {
    testTruncateHelper("string_part", 6, 8, 1);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesStringRgtB() {
    testTruncateHelper("string_part", 7, 6, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesDecimalSame() {
    testTruncateHelper("decimal38_part", 6, 6, 1);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesDecimalRltB() {
    testTruncateHelper("decimal38_part", 6, 8, 0);
  }

  @Test
  public void testEliminateInvalidPartitionCandidatesDecimalRgtB() {
    testTruncateHelper("decimal38_part", 7, 6, 0);
  }

  public void testTruncateHelper(
      final String column_name,
      final int reflectionWidth,
      final int baseWidth,
      final int expectedSize) {
    final PartitionSpec partitionSpecReflection =
        PartitionSpec.builderFor(icebergSchema).truncate(column_name, reflectionWidth).build();

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo>
        reflectionPartitionColumn2TransformInfo =
            buildReflectionPartitionColumn2TransformInfoMap(partitionSpecReflection);
    reflectionPartitionColumn2TransformInfo.forEach((key, value) -> value.setBaseColumnName(key));

    final PartitionSpec partitionSpecBase =
        PartitionSpec.builderFor(icebergSchema).truncate(column_name, baseWidth).build();

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateInvalidPartitionCandidates(
            reflectionPartitionColumn2TransformInfo, partitionSpecBase);

    assertEquals(expectedSize, result.size());
  }

  @Test
  public void testPartitionCandidatesDateDayAndMonth() {
    final PartitionSpec partitionSpecLow =
        PartitionSpec.builderFor(icebergSchema).day("date_part").build();

    final PartitionSpec partitionSpecHigh =
        PartitionSpec.builderFor(icebergSchema).month("date_part").build();
    testPartitionCandidatesHelper(partitionSpecLow, partitionSpecHigh, 0);
    testPartitionCandidatesHelper(partitionSpecHigh, partitionSpecLow, 1);
  }

  @Test
  public void testPartitionCandidatesTimestampDayAndMonth() {
    final PartitionSpec partitionSpecLow =
        PartitionSpec.builderFor(icebergSchema).day("timestamp_part").build();

    final PartitionSpec partitionSpecHigh =
        PartitionSpec.builderFor(icebergSchema).month("timestamp_part").build();
    testPartitionCandidatesHelper(partitionSpecLow, partitionSpecHigh, 0);
    testPartitionCandidatesHelper(partitionSpecHigh, partitionSpecLow, 1);
  }

  @Test
  public void testPartitionCandidatesTimestampIdentityAndMonth() {
    final PartitionSpec partitionSpecLow =
        PartitionSpec.builderFor(icebergSchema).identity("timestamp_part").build();

    final PartitionSpec partitionSpecHigh =
        PartitionSpec.builderFor(icebergSchema).month("timestamp_part").build();
    testPartitionCandidatesHelper(partitionSpecLow, partitionSpecHigh, 0);
    testPartitionCandidatesHelper(partitionSpecHigh, partitionSpecLow, 1);
  }

  public void testPartitionCandidatesHelper(
      final PartitionSpec partitionSpecReflection,
      final PartitionSpec partitionSpecBase,
      final int expectedSize) {

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo>
        reflectionPartitionColumn2TransformInfo =
            buildReflectionPartitionColumn2TransformInfoMap(partitionSpecReflection);
    reflectionPartitionColumn2TransformInfo.forEach((key, value) -> value.setBaseColumnName(key));

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateInvalidPartitionCandidates(
            reflectionPartitionColumn2TransformInfo, partitionSpecBase);

    assertEquals(expectedSize, result.size());
  }

  @Test
  public void testEliminateSimilarPartitionCandidatesEmpty() {
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> input = new HashMap<>();
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateSimilarPartitionCandidates(input);
    assertEquals(0, result.size());
  }

  @Test
  public void testEliminateSimilarPartitionCandidatesUnique() {
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> input = new HashMap<>();
    final IncrementalReflectionByPartitionUtils.TransformInfo dayTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.DAY, "base_col", null, Types.TimeType.get());
    input.put("reflection_col", dayTransformInfo);
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateSimilarPartitionCandidates(input);
    assertEquals(1, result.size());
    assertEquals(dayTransformInfo, result.get("reflection_col"));
  }

  @Test
  public void testEliminateSimilarPartitionCandidatesDifferent() {
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> input = new HashMap<>();
    final IncrementalReflectionByPartitionUtils.TransformInfo dayTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.DAY, "base_col", null, Types.TimeType.get());
    final IncrementalReflectionByPartitionUtils.TransformInfo monthTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.MONTH, "base_col", null, Types.TimeType.get());
    final IncrementalReflectionByPartitionUtils.TransformInfo yearTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.YEAR, "base_col", null, Types.TimeType.get());
    input.put("reflection_col1", dayTransformInfo);
    input.put("reflection_col2", monthTransformInfo);
    input.put("reflection_col3", yearTransformInfo);

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateSimilarPartitionCandidates(input);
    assertEquals(1, result.size());
    assertEquals(dayTransformInfo, result.get("reflection_col1"));
  }

  @Test
  public void testEliminateSimilarPartitionCandidatesIdentity() {
    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> input = new HashMap<>();
    final IncrementalReflectionByPartitionUtils.TransformInfo dayTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.DAY, "base_col", null, Types.TimeType.get());
    final IncrementalReflectionByPartitionUtils.TransformInfo identityTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.IDENTITY,
            "base_col",
            null,
            Types.TimeType.get());
    final IncrementalReflectionByPartitionUtils.TransformInfo yearTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            PartitionProtobuf.IcebergTransformType.YEAR, "base_col", null, Types.TimeType.get());
    input.put("reflection_col1", dayTransformInfo);
    input.put("reflection_col2", identityTransformInfo);
    input.put("reflection_col3", yearTransformInfo);

    final Map<String, IncrementalReflectionByPartitionUtils.TransformInfo> result =
        eliminateSimilarPartitionCandidates(input);
    // nothing truncated, they are not compatible
    assertEquals(1, result.size());
    assertEquals(identityTransformInfo, result.get("reflection_col2"));
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoIdentity() {
    final String baseColumnName = "base_col";

    // the bottom is identity, the result is the same as the top
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        null,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata1);
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoDiffTableMetadata() {
    final String baseColumnName = "base_col";

    // the table in the input ref and the table we are looking for a different
    // no matching transform is found, expectedTransform is always null
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        null,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata2);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        null,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata2);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        null,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata2);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        null,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata2);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        null,
        null,
        null,
        createRexTableInputRef(baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE)),
        tableMetadata2);
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoToDate() {
    final String baseColumnName = "base_col";

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createToDateFunc(baseColumnName),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createToDateFunc(baseColumnName),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createToDateFunc(baseColumnName),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createToDateFunc(baseColumnName),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        null,
        null,
        null,
        createToDateFunc(baseColumnName),
        tableMetadata1);
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoTruncateLeft() {
    final String baseColumnName = "string_part";

    // the bottom is LEFT(COL, 5) , top is identity
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        5,
        createLeftFunc(baseColumnName, 5),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        3,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        3,
        createLeftFunc(baseColumnName, 5),
        tableMetadata1);

    // this fails because top is greater length than bottom
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        7,
        null,
        null,
        createLeftFunc(baseColumnName, 5),
        tableMetadata1);

    // this fails as length is negative
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        3,
        null,
        3,
        createLeftFunc(baseColumnName, -5),
        tableMetadata1);

    // this fails as length is zero
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.TRUNCATE,
        3,
        null,
        3,
        createLeftFunc(baseColumnName, 0),
        tableMetadata1);
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoTruncateSubstr() {
    final String baseColumnName = "string_part";
    final List<String> funcNames = new ArrayList<>(ImmutableList.of("SUBSTR", "SUBSTRING"));
    for (final String funcName : funcNames) {
      // the bottom is SUBSTR(COL, 0, 5) , top is identity
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.IDENTITY,
          null,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          5,
          createSubstrFunc(baseColumnName, 5, 0, funcName),
          tableMetadata1);

      // the bottom is SUBSTR(COL, 0, 5) , top is TRUNCATE(3)
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          3,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          3,
          createSubstrFunc(baseColumnName, 5, 0, funcName),
          tableMetadata1);

      // this fails because top is greater length than bottom
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          7,
          null,
          null,
          createSubstrFunc(baseColumnName, 5, 0, funcName),
          tableMetadata1);

      // this will not work, as beginning offset is non-zero
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          3,
          null,
          null,
          createSubstrFunc(baseColumnName, 5, 1, funcName),
          tableMetadata1);

      // this will not work, as length is negative
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          3,
          null,
          null,
          createSubstrFunc(baseColumnName, -5, 0, funcName),
          tableMetadata1);

      // this will not work, as length is zero
      testGenerateResolvedReflectionTransformInfoHelper(
          baseColumnName,
          PartitionProtobuf.IcebergTransformType.TRUNCATE,
          3,
          null,
          null,
          createSubstrFunc(baseColumnName, 0, 0, funcName),
          tableMetadata1);
    }
  }

  @Test
  public void testGenerateResolvedReflectionTransformInfoDateTrunc() {
    final String baseColumnName = "base_col";

    // test with top identity, bottom changes
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.HOUR,
        null,
        createDateTrunc(baseColumnName, "HOUR"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createDateTrunc(baseColumnName, "DAY"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createDateTrunc(baseColumnName, "MONTH"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.IDENTITY,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createDateTrunc(baseColumnName, "YEAR"),
        tableMetadata1);

    // test with top DAY, bottom changes
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createDateTrunc(baseColumnName, "HOUR"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        createDateTrunc(baseColumnName, "DAY"),
        tableMetadata1);

    // this will not work as bottom is higher than top
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        null,
        null,
        createDateTrunc(baseColumnName, "MONTH"),
        tableMetadata1);

    // this will not work as bottom is higher than top
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.DAY,
        null,
        null,
        null,
        createDateTrunc(baseColumnName, "YEAR"),
        tableMetadata1);

    // test with top MONTH, bottom changes
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createDateTrunc(baseColumnName, "HOUR"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createDateTrunc(baseColumnName, "DAY"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        createDateTrunc(baseColumnName, "MONTH"),
        tableMetadata1);

    // this will not work as bottom is higher than top
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.MONTH,
        null,
        null,
        null,
        createDateTrunc(baseColumnName, "YEAR"),
        tableMetadata1);

    // test with top YEAR, bottom changes
    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createDateTrunc(baseColumnName, "HOUR"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createDateTrunc(baseColumnName, "DAY"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createDateTrunc(baseColumnName, "MONTH"),
        tableMetadata1);

    testGenerateResolvedReflectionTransformInfoHelper(
        baseColumnName,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        PartitionProtobuf.IcebergTransformType.YEAR,
        null,
        createDateTrunc(baseColumnName, "YEAR"),
        tableMetadata1);
  }

  public void testGenerateResolvedReflectionTransformInfoHelper(
      final String baseColumnName,
      final PartitionProtobuf.IcebergTransformType topTransform,
      final Integer topArg,
      final PartitionProtobuf.IcebergTransformType expectedTransform,
      final Integer expectedArg,
      final RexNode rexNode,
      TableMetadata tableMetadata) {
    final IncrementalReflectionByPartitionUtils.TransformInfo identityTransformInfo =
        new IncrementalReflectionByPartitionUtils.TransformInfo(
            topTransform, baseColumnName, topArg, Types.StringType.get());
    final IncrementalReflectionByPartitionUtils.TransformInfo result =
        generateResolvedReflectionTransformInfo(identityTransformInfo, rexNode, tableMetadata);
    if (expectedTransform != null) {
      assertEquals(expectedTransform, result.getTransformType());
      assertEquals(baseColumnName, result.getBaseColumnName());
      assertEquals(expectedArg, result.getArgument());
    } else {
      assertNull(result);
    }
  }

  public RexCall createDateTrunc(final String baseColumnName, final String granularity) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final SqlOperator sqlOperator =
        operatorTable.getOperatorList().stream()
            .filter(f -> f.getName().equalsIgnoreCase("DATE_TRUNC"))
            .collect(Collectors.toList())
            .get(0);
    return (RexCall)
        rexBuilder.makeCall(
            sqlOperator,
            ImmutableList.of(
                createRexLiteral(granularity),
                createRexTableInputRef(
                    baseColumnName, typeFactory.createSqlType(SqlTypeName.DATE))));
  }

  public RexCall createLeftFunc(final String baseColumnName, final int length) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final SqlOperator sqlOperator =
        operatorTable.getOperatorList().stream()
            .filter(f -> f.getName().equalsIgnoreCase("LEFT"))
            .collect(Collectors.toList())
            .get(0);
    return (RexCall)
        rexBuilder.makeCall(
            sqlOperator,
            ImmutableList.of(
                createRexTableInputRef(
                    baseColumnName, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                createRexLiteral(length)));
  }

  public RexCall createSubstrFunc(
      final String baseColumnName,
      final int length,
      final int begin_offset,
      final String functionName) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final List<SqlOperator> sqlOperators =
        operatorTable.getOperatorList().stream()
            .filter(f -> f.getName().equalsIgnoreCase(functionName))
            .collect(Collectors.toList());
    for (final SqlOperator sqlOperator : sqlOperators) {
      try {
        return (RexCall)
            rexBuilder.makeCall(
                sqlOperator,
                ImmutableList.of(
                    createRexTableInputRef(
                        baseColumnName, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    createRexLiteral(begin_offset),
                    createRexLiteral(length)));
      } catch (final UserException e) {
        // ignore the exception and move to the next operator
        // substr has many signatures, we want to find the one that matches the passed in signature
      }
    }
    return null;
  }

  public RexLiteral createRexLiteral(final String contents) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    return rexBuilder.makeLiteral(contents);
  }

  public RexLiteral createRexLiteral(final Integer contents) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    return (RexLiteral)
        rexBuilder.makeLiteral(contents, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
  }

  public RexTableInputRef createRexTableInputRef(
      final String baseColumnName, final RelDataType dataType) {
    final RelDataTypeField relDataTypeField = new RelDataTypeFieldImpl(baseColumnName, 0, dataType);
    final RelOptNamespaceTable relOptNamespaceTable = mock(RelOptNamespaceTable.class);
    final RelDataType relDataType = mock(RelDataType.class);
    when(relOptNamespaceTable.getRowType()).thenReturn(relDataType);
    NamespaceTable namespaceTable = Mockito.mock(NamespaceTable.class);
    when(namespaceTable.getDataset()).thenReturn(tableMetadata1);
    when(relOptNamespaceTable.unwrap(any())).thenReturn(namespaceTable);
    when(relDataType.getFieldList()).thenReturn(ImmutableList.of(relDataTypeField));
    final RelTableRef tableRef = RelTableRef.of(relOptNamespaceTable, 0);
    return RexTableInputRef.of(tableRef, 0, dataType);
  }

  public RexCall createToDateFunc(final String baseColumnName) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final List<SqlOperator> sqlOperators =
        operatorTable.getOperatorList().stream()
            .filter(f -> f.getName().equalsIgnoreCase("TO_DATE"))
            .collect(Collectors.toList());
    for (final SqlOperator sqlOperator : sqlOperators) {
      try {
        return (RexCall)
            rexBuilder.makeCall(
                sqlOperator,
                ImmutableList.of(
                    createRexTableInputRef(
                        baseColumnName, typeFactory.createSqlType(SqlTypeName.VARCHAR))));
      } catch (final UserException e) {
        // ignore the exception and move to the next operator
        // substr has many signatures, we want to find the one that matches the passed in signature
      }
    }
    return null;
  }
}
