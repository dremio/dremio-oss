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
package com.dremio.exec.planner.sql.handlers;

import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.DremioJoinPushTransitivePredicatesRule;

/**
 * Bottom up visitor to pull filters up from logical joins.
 */
public class JoinPullTransitiveFiltersVisitor extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(LogicalJoin join) {
    RelNode rel = super.visit(join);
    if (!(rel instanceof LogicalJoin)) {
      return rel;
    }
    RelNode newJoin = DremioJoinPushTransitivePredicatesRule.findAndApplyTransitivePredicates((LogicalJoin) rel);
    return Optional.ofNullable(newJoin).orElse(rel);
  }
}
