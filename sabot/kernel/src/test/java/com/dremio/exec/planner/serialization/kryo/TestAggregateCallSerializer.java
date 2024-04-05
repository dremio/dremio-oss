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

import com.dremio.PlanTestBase;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.kryo.serializers.AggregateCallSerializer;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.objenesis.strategy.StdInstantiatorStrategy;

@RunWith(MockitoJUnitRunner.class)
public class TestAggregateCallSerializer extends PlanTestBase {

  private RelOptCluster cluster;
  private RelDataTypeFactory typeFactory;
  private RelBuilder relBuilder;
  private LogicalPlanSerializer serializer;
  private LogicalPlanDeserializer deserializer;
  private RelDataType rowType;
  private RelNode rel;

  @Before
  public void setup() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    typeFactory = SqlTypeFactoryImpl.INSTANCE;
    cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    KryoRelSerializerFactory kryoRelSerializerFactory = new KryoRelSerializerFactory(null);
    serializer = kryoRelSerializerFactory.getSerializer(cluster, null);
    DremioCatalogReader catalogReader = Mockito.mock(DremioCatalogReader.class);
    deserializer = kryoRelSerializerFactory.getDeserializer(cluster, catalogReader, null);
    rowType =
        typeFactory.createStructType(
            ImmutableList.of(typeFactory.createSqlType(SqlTypeName.BIGINT)),
            ImmutableList.of("num"));
    relBuilder.values(rowType);
    relBuilder.aggregate(
        relBuilder.groupKey(0),
        relBuilder.aggregateCall(
            SqlStdOperatorTable.COUNT,
            false,
            null,
            "cnt",
            cluster.getRexBuilder().makeInputRef(rowType, 0)));
    rel = relBuilder.build();
  }

  @Test
  public void testRoundTrip() {
    final RelNode newRel = deserializer.deserialize(serializer.serializeToBytes(rel));
    final AggregateCall originalCall = ((LogicalAggregate) rel).getAggCallList().get(0);
    final AggregateCall deserializedCall = ((LogicalAggregate) newRel).getAggCallList().get(0);
    Assert.assertEquals(originalCall.getCollation(), deserializedCall.getCollation());
    Assert.assertEquals(originalCall.getArgList(), deserializedCall.getArgList());
    Assert.assertEquals(originalCall.getName(), deserializedCall.getName());
    Assert.assertEquals(originalCall.getAggregation(), deserializedCall.getAggregation());
  }

  @Test
  public void testUpgrade() {
    AggregateCall originalCall = ((LogicalAggregate) rel).getAggCallList().get(0);

    // serialize without collation field
    final Kryo kryo = new Kryo();
    kryo.setInstantiatorStrategy(
        new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    final DremioCatalogReader catalog = kryo.newInstance(DremioCatalogReader.class);
    final KryoRelSerializer serializer =
        KryoRelSerializer.newBuilder(kryo, cluster, catalog).build();
    FieldSerializer<AggregateCall> oldSerde =
        new FieldSerializer<>(serializer.getKryo(), AggregateCall.class);
    oldSerde.removeField("collation");
    kryo.register(AggregateCall.class, oldSerde);
    byte[] serialized = serializer.serialize(rel);

    // deserialize with AggregateCallSerializer and verify it is correctly read
    AggregateCallSerializer newSerde =
        AggregateCallSerializer.of(serializer.getKryo(), AggregateCall.class);
    kryo.register(AggregateCall.class, newSerde);
    AggregateCall deserializedCall =
        ((LogicalAggregate) serializer.deserialize(serialized)).getAggCallList().get(0);
    Assert.assertEquals(RelCollations.EMPTY, deserializedCall.getCollation());
    Assert.assertEquals(originalCall.getArgList(), deserializedCall.getArgList());
    Assert.assertEquals(originalCall.getName(), deserializedCall.getName());
    Assert.assertEquals(originalCall.getAggregation(), deserializedCall.getAggregation());
  }
}
