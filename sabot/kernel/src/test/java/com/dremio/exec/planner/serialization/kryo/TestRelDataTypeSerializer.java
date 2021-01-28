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
package com.dremio.exec.planner.serialization.kryo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.PlanTestBase;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;

public class TestRelDataTypeSerializer extends PlanTestBase {

  private RelOptCluster cluster;
  private RelDataTypeFactory typeFactory;
  private RelBuilder relBuilder;
  private LogicalPlanSerializer serializer;
  private LogicalPlanDeserializer deserializer;

  @Before
  public void setup() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    typeFactory = SqlTypeFactoryImpl.INSTANCE;
    cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    KryoRelSerializerFactory kryoRelSerializerFactory = new KryoRelSerializerFactory(null);
    serializer = kryoRelSerializerFactory.getSerializer(cluster);
    DremioCatalogReader catalogReader = Mockito.mock(DremioCatalogReader.class);
    deserializer = kryoRelSerializerFactory.getDeserializer(cluster, catalogReader, null);
  }

  @Test
  public void testRelDataTypeSerializer() {
    String col1 = UUID.randomUUID().toString();
    String col2 = UUID.randomUUID().toString();
    List<RelDataTypeField> fields = new ArrayList<>();
    RelDataTypeField field0 = new RelDataTypeFieldImpl(
      col1, 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 = new RelDataTypeFieldImpl(
      col2, 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);

    // Create a nullable struct type without using type factory cache
    final RelDataType nullableRecordType = new RelRecordType(StructKind.FULLY_QUALIFIED, fields, true);
    Assert.assertTrue(nullableRecordType.isNullable());

    // Serde through Kryo serializer and double check nullability
    RelNode nullableRel = relBuilder.values(nullableRecordType).build();
    byte[] nullableSerialized = serializer.serializeToBytes(nullableRel);
    RelNode deserializedNullableRel = deserializer.deserialize(nullableSerialized);
    Assert.assertTrue(deserializedNullableRel.getRowType().isNullable());
  }
}
