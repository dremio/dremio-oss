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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.parquet.ParquetColumnIcebergResolver;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.DefaultNameMapping;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergColumnResolver {

  @Test
  public void renameTest() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("col1"));
    paths.add(new SchemaPath("col2"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col2").setId(2).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("col1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("col2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    List<String> expectedIcebergColumns = new ArrayList<>();
    expectedIcebergColumns.add("col1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), expectedIcebergColumns);

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
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(2).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p3").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    List<String> expectedIcebergColumns = new ArrayList<>();
    expectedIcebergColumns.add("p1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), expectedIcebergColumns);

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
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(2).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
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
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p3").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals(null, columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

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

    columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p3", columnIcebergResolver.getBatchSchemaColumnName("p3"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

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
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("p2").setId(3).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("p1", 1);
    parquetColumnIDs.put("p2", 2);

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals(null, columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    List<String> parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

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

    columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("p1", columnIcebergResolver.getBatchSchemaColumnName("p1"));
    Assert.assertEquals("p2", columnIcebergResolver.getBatchSchemaColumnName("p2"));

    Assert.assertEquals(columnIcebergResolver.getBatchSchemaProjectedColumns(), paths);
    parquetColumns = new ArrayList<>();
    parquetColumns.add("p1");

    Assert.assertEquals(
        columnIcebergResolver.getBatchSchemaColumnName(parquetColumns), parquetColumns);

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
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f1").setId(8).build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f2").setId(9).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.element").setId(4).build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder.setSchemaPath("col4.f2.element").setId(7).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col4.f1").setId(5).build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder.setSchemaPath("col4.f2.sub_f2.element").setId(10).build());
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

    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, parquetColumnIDs);
    Assert.assertEquals("col1", columnIcebergResolver.getBatchSchemaColumnName("col1"));
    Assert.assertEquals(
        "col4.f2.sub_f2",
        columnIcebergResolver.getBatchSchemaColumnName("col2.list.element.f2.list.element.sub_f2"));
  }

  @Test
  public void nestedFieldTestWithoutBatchSchemaForResolution() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(SchemaPath.getCompoundPath("col5", "f2"));
    paths.add(SchemaPath.getCompoundPath("col3"));
    paths.add(SchemaPath.getCompoundPath("col5", "f3"));
    // col5.f2[8]
    PathSegment.NameSegment indexedSegment =
        new PathSegment.NameSegment(
            "col5", new PathSegment.NameSegment("f2", new PathSegment.ArraySegment(8)));
    paths.add(new SchemaPath(indexedSegment));
    ParquetColumnIcebergResolver columnIcebergResolver = getParquetColumnResolver(paths, false);
    Assert.assertEquals(columnIcebergResolver.getProjectedParquetColumns().size(), paths.size());
    Assert.assertEquals(
        "col2.f2", columnIcebergResolver.getProjectedParquetColumns().get(0).toDotString());
    Assert.assertEquals(
        "col3", columnIcebergResolver.getProjectedParquetColumns().get(1).toDotString());
    Assert.assertEquals(
        "col5.f3", columnIcebergResolver.getProjectedParquetColumns().get(2).toDotString());
    Assert.assertEquals(
        "col2.f2.list.element",
        columnIcebergResolver.getProjectedParquetColumns().get(3).toDotString());
  }

  @Test
  public void nestedFieldTestWithIndexAndStruct() {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(SchemaPath.getCompoundPath("col5", "f2", "sub_f1"));
    paths.add(SchemaPath.getCompoundPath("col5", "f2", "sub_f2"));
    paths.add(SchemaPath.getCompoundPath("col3"));
    paths.add(SchemaPath.getCompoundPath("col5", "f3"));
    // col5.f2[8]
    PathSegment.NameSegment indexedSegment =
        new PathSegment.NameSegment(
            "col5", new PathSegment.NameSegment("f2", new PathSegment.ArraySegment(8)));
    paths.add(new SchemaPath(indexedSegment));
    ParquetColumnIcebergResolver columnIcebergResolver = getParquetColumnResolver(paths, true);
    Assert.assertEquals(columnIcebergResolver.getProjectedParquetColumns().size(), paths.size());
    Assert.assertEquals(
        "col2.f2.list.element.list.element.sub_f1",
        columnIcebergResolver.getProjectedParquetColumns().get(0).toDotString());
    Assert.assertEquals(
        "col2.f2.list.element.list.element.sub_f2",
        columnIcebergResolver.getProjectedParquetColumns().get(1).toDotString());
    Assert.assertEquals(
        "col3", columnIcebergResolver.getProjectedParquetColumns().get(2).toDotString());
    Assert.assertEquals(
        "col5.f3", columnIcebergResolver.getProjectedParquetColumns().get(3).toDotString());
    Assert.assertEquals(
        "col2.f2.list.element",
        columnIcebergResolver.getProjectedParquetColumns().get(4).toDotString());
  }

  private ParquetColumnIcebergResolver getParquetColumnResolver(
      List<SchemaPath> paths, boolean shouldUseBatchSchemaForResolvingProjectedColumn) {
    // Generating batch schema:
    // col5 row< f2: Array<Array<Row(sub_f1:varchar, sub_f2:int)>>>, element >
    Field sub_f2 = new Field("sub_f2", FieldType.nullable(new ArrowType.Int(16, false)), null);
    Field sub_f1 = new Field("sub_f1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
    List<Field> struct_fields = new ArrayList<>();
    struct_fields.add(sub_f2);
    struct_fields.add(sub_f1);
    Field structField =
        new Field("$data$", FieldType.nullable(ArrowType.Struct.INSTANCE), struct_fields);
    Field listField1 =
        new Field(
            "$data$",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(structField));
    structField =
        new Field(
            "f2",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(listField1));
    Field col5 =
        new Field(
            "col5",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(structField));
    Field col3 = new Field("col3", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
    BatchSchema schema = BatchSchema.of(col5, col3);

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder
            .setSchemaPath("col5.f2.list.element.list.element.sub_f1")
            .setId(8)
            .build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder
            .setSchemaPath("col5.f2.list.element.list.element.sub_f2")
            .setId(9)
            .build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder.setSchemaPath("col5.f2.list.element").setId(7).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col5.f3").setId(13).build());
    icebergColumnIDs.add(
        icebergSchemaFieldBuilder
            .setSchemaPath("col5.f2.list.element.list.element")
            .setId(10)
            .build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col5.f2").setId(6).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col5").setId(2).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col3").setId(3).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col1").setId(1).build());

    Map<String, Integer> parquetColumnIDs = new HashMap<>();
    parquetColumnIDs.put("col2.f2.list.element", 7);
    parquetColumnIDs.put("col2.f2.list.element.list.element.sub_f1", 8);
    parquetColumnIDs.put("col2.f2.list.element.list.element.sub_f2", 9);
    parquetColumnIDs.put("col2.f2", 6);
    parquetColumnIDs.put("col5.f3", 13);
    parquetColumnIDs.put("col2", 2);
    parquetColumnIDs.put("col3", 3);
    parquetColumnIDs.put("col1", 1);
    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(
            null,
            paths,
            icebergColumnIDs,
            null,
            parquetColumnIDs,
            shouldUseBatchSchemaForResolvingProjectedColumn,
            schema);
    return columnIcebergResolver;
  }

  private void testSchemaNameMappingDefault(
      List<DefaultNameMapping> icebergDefaultNameMapping,
      Map<String, Integer> parquetColumnIDs,
      PrimitiveType parquetColumn1,
      PrimitiveType parquetColumn2) {
    List<SchemaPath> paths = new ArrayList<>();
    paths.add(new SchemaPath("col1"));
    paths.add(new SchemaPath("col2"));

    List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs = new ArrayList<>();
    IcebergProtobuf.IcebergSchemaField.Builder icebergSchemaFieldBuilder =
        IcebergProtobuf.IcebergSchemaField.newBuilder();
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col1").setId(1).build());
    icebergColumnIDs.add(icebergSchemaFieldBuilder.setSchemaPath("col2").setId(2).build());

    // parquet columns "p1" and "p2" are not shown in parquetColumnIDs since they dont have Ids
    ParquetColumnIcebergResolver columnIcebergResolver =
        new ParquetColumnIcebergResolver(paths, icebergColumnIDs, null, ImmutableMap.of());
    // can't find "p1" and "p2" since they are defined nowhere
    assertThat(columnIcebergResolver.getBatchSchemaColumnName(parquetColumn1.getName())).isNull();
    assertThat(columnIcebergResolver.getBatchSchemaColumnName(parquetColumn2.getName())).isNull();

    MessageType parquetSchema = new MessageType("root", parquetColumn2, parquetColumn1);
    columnIcebergResolver =
        new ParquetColumnIcebergResolver(
            parquetSchema,
            paths,
            icebergColumnIDs,
            icebergDefaultNameMapping,
            parquetColumnIDs,
            false,
            null);
    // find "p1" and "p2" since they are defined in icebergDefaultNameMapping
    Assert.assertEquals(
        "col1", columnIcebergResolver.getBatchSchemaColumnName(parquetColumn1.getName()));
    Assert.assertEquals(
        "col2", columnIcebergResolver.getBatchSchemaColumnName(parquetColumn2.getName()));

    // check projectedParquetColumns, the order of the column is the same as projected schema
    // columns (not parquet columns in parquet file)
    List<SchemaPath> expectedProjectedParquetColumns =
        ImmutableList.of(
            SchemaPath.getSimplePath(parquetColumn1.getName()),
            SchemaPath.getSimplePath(parquetColumn2.getName()));
    Assert.assertEquals(
        expectedProjectedParquetColumns, columnIcebergResolver.getProjectedParquetColumns());
  }

  @Test
  public void testSchemaNameMappingDefault() {
    // add parquet columns "p1" and "p2" to icebergDefaultNameMapping
    List<DefaultNameMapping> icebergDefaultNameMapping =
        ImmutableList.of(
            DefaultNameMapping.newBuilder().setName("p1").setId(1).build(),
            DefaultNameMapping.newBuilder().setName("p2").setId(2).build());

    // Create parquet columns without Ids
    PrimitiveType p1 = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "p1");
    PrimitiveType p2 = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "p2");

    testSchemaNameMappingDefault(icebergDefaultNameMapping, ImmutableMap.of(), p1, p2);
  }

  @Test
  /***
   *     this is reproing on Iceberg tables converted by Unity (DX-93899) where:
   *     - icebergDefaultNameMapping is a copy of icebergColumnIDs, and
   *     - parquet columns contain Ids, and
   *     - parquet columns are mapped to Iceberg columns by Ids
   */
  public void testSchemaNameMappingDefaultWithParquetColumnIds() {
    // add icebergColumnIDs name/id to icebergDefaultNameMapping
    List<DefaultNameMapping> icebergDefaultNameMapping =
        ImmutableList.of(
            DefaultNameMapping.newBuilder().setName("col1").setId(1).build(),
            DefaultNameMapping.newBuilder().setName("col2").setId(2).build());

    // Create parquet columns with Ids
    PrimitiveType p1 =
        new PrimitiveType(
            Repetition.OPTIONAL, PrimitiveTypeName.INT32, 0, "p1", null, null, new Type.ID(1));
    PrimitiveType p2 =
        new PrimitiveType(
            Repetition.OPTIONAL, PrimitiveTypeName.INT32, 0, "p2", null, null, new Type.ID(2));

    testSchemaNameMappingDefault(
        icebergDefaultNameMapping, ImmutableMap.of("p1", 1, "p2", 2), p1, p2);
  }
}
