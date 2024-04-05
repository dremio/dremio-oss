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
package com.dremio.exec.planner.serializer.logical;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PCorrelationId;
import com.dremio.plan.serialization.PLogicalCorrelate;
import com.dremio.plan.serialization.PLogicalCorrelate.PSemiJoinType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.util.ImmutableBitSet;

/** Serde for LogicalCorrelate */
public final class LogicalCorrelateSerde
    implements RelNodeSerde<LogicalCorrelate, PLogicalCorrelate> {
  @Override
  public PLogicalCorrelate serialize(LogicalCorrelate correlate, RelNodeSerde.RelToProto s) {
    return PLogicalCorrelate.newBuilder()
        .setLeftInput(s.toProto(correlate.getLeft()))
        .setRightInput(s.toProto(correlate.getRight()))
        .setSemiJoinType(toProto(correlate.getJoinType()))
        .setCorrelationId(toProto(correlate.getCorrelationId()))
        .setRequiredColumns(LogicalAggregateSerde.toProto(correlate.getRequiredColumns()))
        .build();
  }

  @Override
  public LogicalCorrelate deserialize(PLogicalCorrelate node, RelFromProto s) {
    RelNode left = s.toRel(node.getLeftInput());
    RelNode right = s.toRel(node.getRightInput());
    JoinRelType joinType = fromProto(node.getSemiJoinType());
    CorrelationId correlationId = fromProto(node.getCorrelationId());
    return LogicalCorrelate.create(
        left,
        right,
        correlationId,
        ImmutableBitSet.of(node.getRequiredColumns().getGroupList()),
        joinType);
  }

  private static JoinRelType fromProto(PSemiJoinType type) {
    switch (type) {
      case INNER:
        return JoinRelType.INNER;
      case LEFT:
        return JoinRelType.LEFT;
      case SEMI:
        return JoinRelType.SEMI;
      case ANTI:
        return JoinRelType.ANTI;
      default:
        throw new UnsupportedOperationException(String.format("Unknown type %s.", type));
    }
  }

  private static PSemiJoinType toProto(JoinRelType type) {
    switch (type) {
      case INNER:
        return PSemiJoinType.INNER;
      case LEFT:
        return PSemiJoinType.LEFT;
      case SEMI:
        return PSemiJoinType.SEMI;
      case ANTI:
        return PSemiJoinType.ANTI;
      default:
        throw new UnsupportedOperationException(String.format("Unknown type %s.", type));
    }
  }

  private static PCorrelationId toProto(CorrelationId correlationId) {
    return PCorrelationId.newBuilder()
        .setColumnIndex(LogicalAggregateSerde.toProto(correlationId.getColumnIndex()))
        .setId(correlationId.getId())
        .build();
  }

  private static CorrelationId fromProto(PCorrelationId correlationId) {
    CorrelationId cId = new CorrelationId(correlationId.getId());
    cId.setColumnIndex(ImmutableBitSet.of(correlationId.getColumnIndex().getGroupList()));
    return cId;
  }
}
