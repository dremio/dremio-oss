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
import com.dremio.plan.serialization.PLogicalFilter;
import com.dremio.plan.serialization.PRexNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;

/** Serde for LogicalFilter */
public final class LogicalFilterSerde implements RelNodeSerde<LogicalFilter, PLogicalFilter> {
  @Override
  public PLogicalFilter serialize(LogicalFilter node, RelToProto s) {
    int input = s.toProto(node.getInput());
    PRexNode condition = s.toProto(node.getCondition());
    return PLogicalFilter.newBuilder().setInput(input).setCondition(condition).build();
  }

  @Override
  public LogicalFilter deserialize(PLogicalFilter node, RelFromProto s) {
    RelNode input = s.toRel(node.getInput());
    RexNode condition = s.toRex(node.getCondition());
    return LogicalFilter.create(input, condition);
  }
}
