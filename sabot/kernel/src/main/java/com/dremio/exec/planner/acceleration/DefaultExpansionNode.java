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
package com.dremio.exec.planner.acceleration;

import java.util.List;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.service.namespace.NamespaceKey;

/**
  * Represents a location where the query was expanded from a VDS to a default reflection
  */
public class DefaultExpansionNode extends ExpansionNode {
  protected DefaultExpansionNode(NamespaceKey path, RelDataType rowType, RelOptCluster cluster, RelTraitSet traits, RelNode input, boolean contextSensitive) {
    super(path, rowType, cluster, traits, input, contextSensitive);
  }

  public static DefaultExpansionNode wrap(NamespaceKey path, RelNode node, RelDataType rowType, boolean contextSensitive) {
    return new DefaultExpansionNode(path, rowType, node.getCluster(), node.getTraitSet(), node, contextSensitive);
  }

  @Override
  public boolean isDefault() {
    return true;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DefaultExpansionNode(getPath(), rowType, this.getCluster(), traitSet, inputs.get(0), isContextSensitive());
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new DefaultExpansionNode(getPath(), rowType, copier.getCluster(), copier.copyOf(getTraitSet()), getInput().accept(copier), isContextSensitive());
  }
}
