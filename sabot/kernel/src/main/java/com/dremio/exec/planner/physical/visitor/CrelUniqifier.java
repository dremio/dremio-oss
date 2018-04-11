/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.physical.visitor;

import java.util.Set;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.RoutingShuttle;
import com.google.common.collect.Sets;

public class CrelUniqifier extends RoutingShuttle {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CrelUniqifier.class);

  private final Set<RelNode> data = Sets.newIdentityHashSet();

  public static RelNode uniqifyGraph(RelNode rel) {
    CrelUniqifier u = new CrelUniqifier();
    return rel.accept(u);
  }

  @Override
  public RelNode visit(RelNode other) {
    if(!data.add(other)) {
      other = other.copy(other.getTraitSet(), other.getInputs());
    }

    return super.visit(other);
  }
}
