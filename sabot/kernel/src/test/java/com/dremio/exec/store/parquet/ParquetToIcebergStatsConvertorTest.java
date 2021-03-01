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
package com.dremio.exec.store.parquet;

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.struct;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.io.Resources;

public class ParquetToIcebergStatsConvertorTest {
  private static OperatorContext context;

  @BeforeClass
  public static void beforeClass() throws Exception {
    context = mock(OperatorContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_ICEBERG_MIN_MAX)).thenReturn(true);
    when(context.getOptions()).thenReturn(optionManager);
  }

  @Test
  public void testMinMaxWhenFileContainsNoBlocks() {
    ParquetMetadata parquetMetadata = new ParquetMetadata(null, new ArrayList<>());
    assertNoMinMax(parquetMetadata, null);
  }

  @Test
  public void testMinMaxWhenBlocksContainNoColumns() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(new BlockMetaData());
    blocks.add(new BlockMetaData());
    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);

    assertNoMinMax(parquetMetadata, null);
  }

  @Test
  public void testMinMaxWhenColumnsHaveNoStats() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(floatBlock());
    blocks.add(intBlock());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get()),
      required(2, "currency", FloatType.get())
    );

    assertNoMinMax(parquetMetadata, schema);
  }

  @Test
  public void testMinMaxForBooleanColumn() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(booleanBlockWithMinMax());
    blocks.add(booleanBlockWithMinMax2());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "eligible", BooleanType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals(0, metrics.lowerBounds().get(1).get(0));
    assertEquals(1, metrics.upperBounds().get(1).get(0));
  }

  @Test
  public void testMinMaxForIntColumn() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(intBlockWithMinMax());
    blocks.add(intBlockWithMinMax2());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals(-1, metrics.lowerBounds().get(1).getInt(0));
    assertEquals(10000, metrics.upperBounds().get(1).getInt(0));
  }

  @Test
  public void testMinMaxForFloatColumn() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(floatBlockWithMinMax());
    blocks.add(floatBlockWithMinMax2());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "currency", FloatType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertTrue(metrics.lowerBounds().get(1).getFloat(0) == -0.1f);
    assertTrue(metrics.upperBounds().get(1).getFloat(0) == 10000.1f);
  }

  @Test
  public void testMinMaxForBinaryColumn() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(binaryBlockWithMinMax());
    blocks.add(binaryBlockWithMinMax2());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "data", BinaryType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals('a', metrics.lowerBounds().get(1).get(0));
    assertEquals('z', metrics.upperBounds().get(1).get(0));
  }

  @Test
  public void testMinMax_oneBlockHasStats() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(intBlockWithMinMax());
    blocks.add(floatBlock());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get()),
      required(2, "currency", FloatType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals(-1, metrics.lowerBounds().get(1).getInt(0));
    assertEquals(1000, metrics.upperBounds().get(1).getInt(0));
  }

  @Test
  public void testMinMaxErasureIfSameColDoesNotHaveStatsInAllBlocks() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(intBlockWithMinMax());
    blocks.add(intBlock());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get())
    );

    assertNoMinMax(parquetMetadata, schema);
  }

  @Test
  public void testMinMaxForDifferentColumnsInDifferentBlocks() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(intBlockWithMinMax());
    blocks.add(floatBlockWithMinMax());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get()),
      required(2, "currency", FloatType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals(-1, metrics.lowerBounds().get(1).getInt(0));
    assertEquals(1000, metrics.upperBounds().get(1).getInt(0));

    assertEquals(-0.1f, metrics.lowerBounds().get(2).getFloat(0), 0.0);
    assertEquals(1000.1f, metrics.upperBounds().get(2).getFloat(0), 0.0);
  }

  @Test
  public void testMinMaxOneBlockHasHeterogeneousColumns() {
    List<BlockMetaData> blocks = new ArrayList<>();
    blocks.add(intFloatBlockWithMinMax());

    ParquetMetadata parquetMetadata = new ParquetMetadata(null, blocks);
    Schema schema = new Schema(
      required(1, "id", IntegerType.get()),
      required(2, "currency", FloatType.get())
    );

    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertEquals(-1, metrics.lowerBounds().get(1).getInt(0));
    assertEquals(1000, metrics.upperBounds().get(1).getInt(0));

    assertEquals(-0.1f, metrics.lowerBounds().get(2).getFloat(0), 0.0);
    assertEquals(1000.1f, metrics.upperBounds().get(2).getFloat(0), 0.0);
  }

  @Test
  public void testNoMinMaxForParquetFileWithList() throws Exception {
    Path path = new Path(Resources.getResource("parquet/array_int_bigint.parquet").toURI());
    BatchSchema batchSchema = BatchSchema.newBuilder()
      .addField(INT.asList().toField("col1"))
      .addField(BIGINT.asList().toField("col2"))
      .build();
    assertNoMinMax(getParquetMetadata(path), SchemaConverter.toIcebergSchema(batchSchema));
  }

  @Test
  public void testNoMinMaxForParquetFileWithStructOfList() throws Exception {
    Path path = new Path(Resources.getResource("parquet/struct_array_int_bigint.parquet").toURI());
    BatchSchema batchSchema = BatchSchema.newBuilder()
      .addField(struct(INT.asList().toField("f1")).toField("col1"))
      .addField(struct(BIGINT.asList().toField("f2")).toField("col2"))
      .build();
    assertNoMinMax(getParquetMetadata(path), SchemaConverter.toIcebergSchema(batchSchema));
  }

  @Test
  public void testNoMinMaxForParquetFileWithStruct() throws Exception {
    Path path = new Path(Resources.getResource("parquet/struct_int_bigint.parquet").toURI());
    BatchSchema batchSchema = BatchSchema.newBuilder()
      .addField(struct(INT.toField("f1")).toField("col1"))
      .addField(struct(BIGINT.toField("f2")).toField("col2"))
      .build();
    assertNoMinMax(getParquetMetadata(path), SchemaConverter.toIcebergSchema(batchSchema));
  }

  @Test
  public void testNoMinMaxForParquetFileWithStructOfStruct() throws Exception {
    Path path = new Path(Resources.getResource("parquet/struct_struct_int_bigint.parquet").toURI());
    BatchSchema batchSchema = BatchSchema.newBuilder()
      .addField(struct(struct(INT.toField("f1")).toField("f1")).toField("col1"))
      .addField(struct(struct(BIGINT.toField("f2")).toField("f2")).toField("col2"))
      .build();
    assertNoMinMax(getParquetMetadata(path), SchemaConverter.toIcebergSchema(batchSchema));
  }

  private ParquetMetadata getParquetMetadata(Path path) throws IOException {
    return ParquetFileReader.readFooter(new Configuration(), path);
  }

  private BlockMetaData intFloatBlockWithMinMax() {
    BlockMetaData blockMetaData = new BlockMetaData();
    IntStatistics intStatistics = new IntStatistics();
    intStatistics.setMinMax(-1, 1000);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("id"), INT32, GZIP, new HashSet<>(), intStatistics,
      1000, 0, 0, 0, 0));

    FloatStatistics floatStatistics = new FloatStatistics();
    floatStatistics.setMinMax(-0.1f, 1000.1f);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("currency"), FLOAT, GZIP, new HashSet<>(), floatStatistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData intBlock() {
    BlockMetaData blockMetaData = new BlockMetaData();
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("id"), INT32, GZIP, new HashSet<>(), null,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData intBlockWithMinMax() {
    BlockMetaData blockMetaData = new BlockMetaData();
    IntStatistics statistics = new IntStatistics();
    statistics.setMinMax(-1, 1000);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("id"), INT32, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData intBlockWithMinMax2() {
    BlockMetaData blockMetaData = new BlockMetaData();
    IntStatistics statistics = new IntStatistics();
    statistics.setMinMax(0, 10000);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("id"), INT32, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData binaryBlockWithMinMax() {
    BlockMetaData blockMetaData = new BlockMetaData();
    BinaryStatistics statistics = new BinaryStatistics();
    statistics.setMinMaxFromBytes("a".getBytes(), "h".getBytes());
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("data"), BINARY, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData binaryBlockWithMinMax2() {
    BlockMetaData blockMetaData = new BlockMetaData();
    BinaryStatistics statistics = new BinaryStatistics();
    statistics.setMinMaxFromBytes("c".getBytes(), "z".getBytes());
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("data"), BINARY, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData booleanBlockWithMinMax() {
    BlockMetaData blockMetaData = new BlockMetaData();
    BooleanStatistics statistics = new BooleanStatistics();
    statistics.setMinMax(false, false);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("eligible"), BOOLEAN, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData booleanBlockWithMinMax2() {
    BlockMetaData blockMetaData = new BlockMetaData();
    BooleanStatistics statistics = new BooleanStatistics();
    statistics.setMinMax(true, true);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("eligible"), BOOLEAN, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData floatBlock() {
    BlockMetaData blockMetaData = new BlockMetaData();
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("currency"), FLOAT, GZIP, new HashSet<>(), null,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData floatBlockWithMinMax() {
    BlockMetaData blockMetaData = new BlockMetaData();
    FloatStatistics statistics = new FloatStatistics();
    statistics.setMinMax(-0.1f, 1000.1f);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("currency"), FLOAT, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private BlockMetaData floatBlockWithMinMax2() {
    BlockMetaData blockMetaData = new BlockMetaData();
    FloatStatistics statistics = new FloatStatistics();
    statistics.setMinMax(0.1f, 10000.1f);
    blockMetaData.addColumn(ColumnChunkMetaData.get(ColumnPath.get("currency"), FLOAT, GZIP, new HashSet<>(), statistics,
      1000, 0, 0, 0, 0));
    return blockMetaData;
  }

  private void assertNoMinMax(ParquetMetadata parquetMetadata, Schema schema) {
    Metrics metrics = ParquetToIcebergStatsConvertor.toMetrics(context, parquetMetadata, schema);
    assertTrue(metrics.lowerBounds().isEmpty());
    assertTrue(metrics.upperBounds().isEmpty());
  }
}