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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.ParquetColumnIcebergResolver;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;

public class TestIcebergColumnResolver {

  @Test
  public void renameTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("col1"));
    paths.add(new SchemaPath("col2"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col2").setId(2).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("col1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("col2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    List<String> expectedIcebergColumns = new ArrayList<>();
    expectedIcebergColumns.add("col1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), expectedIcebergColumns);

    Assert.assertEquals("p1", columnIcebergResolver.getParquetColumnName("col1"));
    Assert.assertEquals("p2", columnIcebergResolver.getParquetColumnName("col2"));

    List<SchemaPath> parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));
    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), paths);
  }

  @Test
  public void addColumnTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("p1"));
    paths.add(new SchemaPath("p2"));
    paths.add(new SchemaPath("p3"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(2).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p3").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    List<String> expectedIcebergColumns = new ArrayList<>();
    expectedIcebergColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), expectedIcebergColumns);

    Assert.assertEquals("p1", columnIcebergResolver.getParquetColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getParquetColumnName("p2"));
    Assert.assertEquals(null, columnIcebergResolver.getParquetColumnName("p3"));

    List<SchemaPath> parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), parquetPaths);
  }

  @Test
  public void dropColumnTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("p2"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(2).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals(null, columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), null);

    Assert.assertEquals("p2", columnIcebergResolver.getParquetColumnName("p2"));

    List<SchemaPath> parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), paths);
  }

  @Test
  public void dropAddColumnTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("p1"));
    paths.add(new SchemaPath("p3"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p3").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals(null, columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

    Assert.assertEquals(null, columnIcebergResolver.getParquetColumnName("p3"));

    List<SchemaPath> parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));

    List<SchemaPath> expectedPaths = new ArrayList<>();
    expectedPaths.add(new SchemaPath("p1"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), expectedPaths);

    // For new parquet file

    parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p3", 3);

    columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p3", columnIcebergResolver.getBatchSchemaColumnName("p3"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

    Assert.assertEquals("p3", columnIcebergResolver.getParquetColumnName("p3"));

    parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p3"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), paths);
  }

  @Test
  public void dropAddSamenameColumnTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("p1"));
    paths.add(new SchemaPath("p2"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals(null, columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

    Assert.assertEquals(null, columnIcebergResolver.getParquetColumnName("p2"));

    List<SchemaPath> parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));

    List<SchemaPath> expectedPaths = new ArrayList<>();
    expectedPaths.add(new SchemaPath("p1"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), expectedPaths);

    // For new parquet file

    parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 3);

    columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

    Assert.assertEquals("p2", columnIcebergResolver.getParquetColumnName("p2"));

    parquetPaths = new ArrayList<>();
    parquetPaths.add(new SchemaPath("p1"));
    parquetPaths.add(new SchemaPath("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaColumns(parquetPaths), paths);
  }

  @Test
  public void nestedFieldTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(SchemaPath.getCompoundPath("col4", "f2", "sub_f1"));
    paths.add(SchemaPath.getCompoundPath("col4", "f2", "sub_f2"));
    paths.add(SchemaPath.getCompoundPath("col4", "element"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder = IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f1").setId(8).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f2").setId(9).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.element").setId(4).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f2.element").setId(7).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f1").setId(5).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f2.element").setId(10).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4").setId(2).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f2").setId(6).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col3").setId(3).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col1").setId(1).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("col2.list.element.f2", 6);
    parquetColumnIDs.put("col2.list.element.f1", 5);
    parquetColumnIDs.put("col2.list.element.f2.list.element.sub_f2", 9);
    parquetColumnIDs.put("col2.list.element.f2.list.element.sub_f1", 8);
    parquetColumnIDs.put("col2.list.element.f2.list.element.sub_f2.list.element", 10);
    parquetColumnIDs.put("col2.list.element.f2.list.element", 7);
    parquetColumnIDs.put("col2", 2);
    parquetColumnIDs.put("col2.list.element", 4);
    parquetColumnIDs.put("col3", 3);
    parquetColumnIDs.put("col1", 1);

    ParquetColumnIcebergResolver columnIcebergResolver = new ParquetColumnIcebergResolver(paths, icebergColumnIDs, parquetColumnIDs);
    Assert.assertEquals("col1", columnIcebergResolver.getBatchSchemaColumnName("col1"));
    Assert.assertEquals("col4.f2.sub_f2", columnIcebergResolver.getBatchSchemaColumnName("col2.list.element.f2.list.element.sub_f2"));
  }

}
