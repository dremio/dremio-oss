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

import com.dremio.exec.planner.serializer.RelNodeSerde.RelToProto;
import com.dremio.plan.serialization.PRelDataType;
import com.dremio.plan.serialization.PRelList;
import com.dremio.plan.serialization.PRexNode;
import com.dremio.plan.serialization.PSqlOperator;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

class RelSerializer implements RelToProto {

  private final List<Any> nodes = new ArrayList<>();
  private final RelSerdeRegistry registry;

  private final RexSerializer rex;
  private final TypeSerde type;
  private final SqlOperatorSerde sqlOperatorSerde;

  public RelSerializer(
      RelSerdeRegistry registry, RelOptCluster cluster, SqlOperatorSerde sqlOperatorSerde) {
    this.registry = registry;
    this.rex =
        new RexSerializer(
            cluster.getRexBuilder(),
            new TypeSerde(cluster.getTypeFactory()),
            registry,
            sqlOperatorSerde);
    this.type = new TypeSerde(cluster.getTypeFactory());
    this.sqlOperatorSerde = sqlOperatorSerde;
  }

  @Override
  public int toProto(RelNode node) {
    RelNodeSerde<?, ?> se = registry.getSerdeByRelNodeClass(node.getClass());
    Message message = se.serializeGeneric(node, this);
    nodes.add(Any.pack(message));
    return nodes.size() - 1;
  }

  @Override
  public SqlOperatorSerde getSqlOperatorSerde() {
    return sqlOperatorSerde;
  }

  @Override
  public PRexNode toProto(RexNode node) {
    return node.accept(rex);
  }

  @Override
  public PRelDataType toProto(RelDataType t) {
    return type.toProto(t);
  }

  @Override
  public PSqlOperator toProto(SqlOperator op) {
    return sqlOperatorSerde.toProto(op);
  }

  public static PRelList serializeList(
      RelSerdeRegistry registry, RelNode node, SqlOperatorSerde sqlOperatorSerde) {
    RelSerializer ser = new RelSerializer(registry, node.getCluster(), sqlOperatorSerde);
    ser.toProto(node);
    return PRelList.newBuilder().addAllNode(ser.nodes).build();
  }

  public List<Any> getNodes() {
    return nodes;
  }
}
