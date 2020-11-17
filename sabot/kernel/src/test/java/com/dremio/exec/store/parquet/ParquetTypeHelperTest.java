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

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.io.file.Path;

/**
 * Tests helper function in ParquetTypeHelper
 */
public class ParquetTypeHelperTest extends BaseTestQuery {

  @Test
  public void testUnWrapVeryComplexSchema() throws Exception{
    // file has following schema
    // col1 array<array<array<struct<f1:array<array<array<int>>>,f2:struct<sub_f1:array<array<array<int>>>,sub_f2:array<array<array<struct<sub_sub_f1:int,sub_sub_f2:string>>>>>>>>>
    // col2 int
    // Total there are 5 leaf level fields:
    // col1[][][].f1[][][], col1[][][].f2.sub_f1[][][], col1[][][].f2.sub_f2[][][].sub_sub_f1, col1[][][].f2.sub_f2[][][].sub_sub_f2, col2
    Set<String> expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f1",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f2",
      "col2"));
    URL complexParquet = getClass().getResource("/parquet/very_complex.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);
    Map<String, ColumnChunkMetaData> unwrapSchema = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), null);
    Assert.assertEquals(expectedKeys, unwrapSchema.keySet());
  }

  @Test
  public void testUnWrapNestedMapSchema() throws Exception{
    // file has following schema
    // col1 map<int,array<map<int,struct<f1:map<int,map<int,array<int>>>>>>>
    // Total there are 5 leaf level fields:
    // col1.key, col1.value[].key, col1.value[].value.f1.key, col1.value[].value.f1.value.key, col1.value[].value.f1.value.value[]
    Set<String> expectedKeys = new HashSet<>(Arrays.asList(
      "col1.map.list.element.key",
      "col1.map.list.element.value.list.element.map.list.element.key",
      "col1.map.list.element.value.list.element.map.list.element.value.f1.map.list.element.key",
      "col1.map.list.element.value.list.element.map.list.element.value.f1.map.list.element.value.map.list.element.key",
      "col1.map.list.element.value.list.element.map.list.element.value.f1.map.list.element.value.map.list.element.value.list.element"
      ));
    URL complexParquet = getClass().getResource("/parquet/mapofmap.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);
    Map<String, ColumnChunkMetaData> unwrapSchema = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), null);
    Assert.assertEquals(expectedKeys, unwrapSchema.keySet());
  }

  @Test
  public void testUnWrapScalarTypes() throws Exception{
    // file has following schema
    // boolean bool_col, int32 int_col, int64 bigint_col, float float4_col, double float8_col, int32 date_col, int64 timestamp_col,
    // int32 time_col, binary varchar_col, binary varbinary_col
    // all are leaf level columns

    Set<String> expectedKeys = new HashSet<>(Arrays.asList(
      "bool_col",
      "int_col",
      "bigint_col",
      "float4_col",
      "float8_col",
      "date_col",
      "timestamp_col",
      "time_col",
      "varchar_col",
      "varbinary_col"
    ));
    URL complexParquet = getClass().getResource("/parquet/all_scalar_types.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);
    Map<String, ColumnChunkMetaData> unwrapSchema = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), null);
    Assert.assertEquals(expectedKeys, unwrapSchema.keySet());
  }

  @Test
  public void testUnwrapSimplePath() throws Exception {
   URL complexParquet = getClass().getResource("/parquet/very_complex.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);

    SchemaPath simplePath = SchemaPath.getSimplePath("col2");

    SortedSet<String> projectedColumns = new TreeSet<>(String::compareToIgnoreCase);
    projectedColumns.add(simplePath.getRootSegment().getPath());

    SortedMap<String, ColumnChunkMetaData> chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(1, chunks.size());
    Assert.assertTrue(chunks.containsKey(simplePath.getRootSegment().getPath()));
    Assert.assertTrue(chunks.containsKey(simplePath.getRootSegment().getPath().toLowerCase()));
    Assert.assertTrue(chunks.containsKey(simplePath.getRootSegment().getPath().toUpperCase()));
  }

  @Test
  public void testUnwrapComplexLeafPath() throws Exception {
    URL complexParquet = getClass().getResource("/parquet/very_complex.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);

    SchemaPath compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
        "list","element","f2","sub_f2","list","element","list","element","list","element","sub_sub_f1");

    SortedSet<String> projectedColumns = new TreeSet<>(String::compareToIgnoreCase);
    projectedColumns.add(compoundPath.toDotString());

    SortedMap<String, ColumnChunkMetaData> chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(1, chunks.size());
    Assert.assertTrue(chunks.containsKey(compoundPath.toDotString()));
    Assert.assertTrue(chunks.containsKey(compoundPath.toDotString().toLowerCase()));
    Assert.assertTrue(chunks.containsKey(compoundPath.toDotString().toUpperCase()));
  }

  @Test
  public void testUnwrapComplexPartialPath() throws Exception {
    URL complexParquet = getClass().getResource("/parquet/very_complex.parquet");
    Path filePath = Path.of(complexParquet.toURI());
    ParquetMetadata parquetMetadata =
      SingletonParquetFooterCache.readFooter(localFs, filePath, ParquetMetadataConverter.NO_FILTER,
        ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getDefault().getNumVal());
    BlockMetaData block = parquetMetadata.getBlocks().get(0);

    SchemaPath compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element");

    Set<String> expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f1",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f2"));

    SortedSet<String> projectedColumns = new TreeSet<>(String::compareToIgnoreCase);
    projectedColumns.add(compoundPath.toDotString());

    SortedMap<String, ColumnChunkMetaData> chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(4, chunks.size());
    Assert.assertEquals(expectedKeys, chunks.keySet());

    // select f1

    compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element", "f1");

    expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f1.list.element.list.element.list.element"));

    projectedColumns.clear();
    projectedColumns.add(compoundPath.toDotString());

    chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(1, chunks.size());
    Assert.assertEquals(expectedKeys, chunks.keySet());

    // select f2

    compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element", "f2");

    expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f2.sub_f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f1",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f2"));

    projectedColumns.clear();
    projectedColumns.add(compoundPath.toDotString());

    chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(3, chunks.size());
    Assert.assertEquals(expectedKeys, chunks.keySet());

    // select f2.sub_f2

    compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element", "f2", "sub_f2");

    expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f1",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f2"));

    projectedColumns.clear();
    projectedColumns.add(compoundPath.toDotString());

    chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(2, chunks.size());
    Assert.assertEquals(expectedKeys, chunks.keySet());

    // select f2.sub_f2, f2.sub_f1

    projectedColumns.clear();
    compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element", "f2", "sub_f2");
    projectedColumns.add(compoundPath.toDotString());
    compoundPath = SchemaPath.getCompoundPath("col1","list","element","list","element",
      "list","element", "f2", "sub_f1");
    projectedColumns.add(compoundPath.toDotString());
    compoundPath = SchemaPath.getSimplePath("col2");
    projectedColumns.add(compoundPath.toDotString());
    compoundPath = SchemaPath.getSimplePath("nonexistingcolumn");
    projectedColumns.add(compoundPath.toDotString());

    expectedKeys = new HashSet<>(Arrays.asList(
      "col1.list.element.list.element.list.element.f2.sub_f1.list.element.list.element.list.element",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f1",
      "col1.list.element.list.element.list.element.f2.sub_f2.list.element.list.element.list.element.sub_sub_f2",
      "col2"));

    chunks = ParquetTypeHelper.unWrapParquetSchema(block, parquetMetadata.getFileMetaData().getSchema(), projectedColumns);
    Assert.assertEquals(4, chunks.size());
    Assert.assertEquals(expectedKeys, chunks.keySet());

  }
}
