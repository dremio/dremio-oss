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
package com.dremio.exec.planner.serializer;

import com.dremio.exec.planner.serializer.RelNodeSerde.PluginRetriever;
import com.dremio.exec.planner.serializer.RelNodeSerde.RelFromProto;
import com.dremio.exec.planner.serializer.RelNodeSerde.TableRetriever;
import com.dremio.exec.planner.sql.DremioToRelContext;
import com.dremio.plan.serialization.PRelDataType;
import com.dremio.plan.serialization.PRelList;
import com.dremio.plan.serialization.PRexNode;
import com.dremio.plan.serialization.PRexWindowBound;
import com.dremio.plan.serialization.PSqlOperator;
import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

class RelDeserializer implements RelFromProto {

  private final RelSerdeRegistry registry;
  private final RelBuilderFactory factory;
  private final TableRetriever tables;
  private final PluginRetriever plugins;
  private final RelOptCluster cluster;
  private final List<Any> nodes;
  private final RexDeserializer rexDeserializer;
  private final TypeSerde types;

  private final SqlOperatorSerde sqlOperatorSerde;

  public RelDeserializer(
      RelSerdeRegistry registry,
      RelBuilderFactory factory,
      TableRetriever tables,
      PluginRetriever plugins,
      RelOptCluster cluster,
      List<Any> nodes,
      SqlOperatorSerde sqlOperatorSerde) {
    this.registry = registry;
    this.factory = factory;
    this.tables = tables;
    this.plugins = plugins;
    this.cluster = cluster;
    this.nodes = nodes;
    this.types = new TypeSerde(cluster.getTypeFactory());
    this.sqlOperatorSerde = sqlOperatorSerde;
    this.rexDeserializer =
        new RexDeserializer(
            cluster.getRexBuilder(), types, registry, tables, plugins, cluster, sqlOperatorSerde);
  }

  @Override
  public RelNode toRel(int index) {
    final Any any = Preconditions.checkNotNull(nodes.get(index));
    RelNode node = registry.getSerdeByTypeString(any.getTypeUrl()).deserialize(any, this);
    return node;
  }

  @Override
  public RexNode toRex(PRexNode rex) {
    Preconditions.checkNotNull(rex);
    return rexDeserializer.convert(rex);
  }

  @Override
  public RexWindowBound toRex(PRexWindowBound pRexWindowBound) {
    Preconditions.checkNotNull(pRexWindowBound);
    return rexDeserializer.convert(pRexWindowBound);
  }

  @Override
  public RexNode toRex(PRexNode rex, RelDataType rowType) {
    return rexDeserializer.convert(rex, rowType);
  }

  @Override
  public SqlOperator toOp(PSqlOperator op) {
    return sqlOperatorSerde.fromProto(op);
  }

  @Override
  public ToRelContext toRelContext() {
    return DremioToRelContext.createSerializationContext(cluster);
  }

  @Override
  public RelDataType toRelDataType(PRelDataType type) {
    TypeSerde s = new TypeSerde(cluster.getTypeFactory());
    return s.fromProto(type);
  }

  @Override
  public RelBuilder builder() {
    return factory.create(cluster, null);
  }

  @Override
  public TableRetriever tables() {
    return tables;
  }

  @Override
  public PluginRetriever plugins() {
    return plugins;
  }

  @Override
  public RelOptCluster cluster() {
    return cluster;
  }

  public static RelNode deserialize(
      RelSerdeRegistry registry,
      RelBuilderFactory factory,
      TableRetriever tables,
      PluginRetriever plugins,
      PRelList list,
      RelOptCluster cluster,
      SqlOperatorSerde sqlOperatorSerde) {
    RelDeserializer de =
        new RelDeserializer(
            registry, factory, tables, plugins, cluster, list.getNodeList(), sqlOperatorSerde);
    return de.toRel(list.getNodeCount() - 1);
  }
}
