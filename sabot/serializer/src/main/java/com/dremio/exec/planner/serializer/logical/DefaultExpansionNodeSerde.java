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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.planner.acceleration.DefaultExpansionNode;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PDefaultExpansionNode;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Strings;

/**
 * Serde for {@link DefaultExpansionNode}
 */
public final class DefaultExpansionNodeSerde implements RelNodeSerde<DefaultExpansionNode, PDefaultExpansionNode> {
  @Override
  public PDefaultExpansionNode serialize(DefaultExpansionNode expansionNode, RelToProto s) {
    List<String> path = expansionNode.getPath().getPathComponents();
    PDefaultExpansionNode.Builder builder = PDefaultExpansionNode.newBuilder()
      .setInput(s.toProto(expansionNode.getInput()))
      .addAllPath(path)
      .setContextSensitive(expansionNode.isContextSensitive());
    if (expansionNode.getVersionContext() != null) {
      builder.setVersionContext(expansionNode.getVersionContext().serialize());
    }
    return builder.build();
  }

  @Override
  public DefaultExpansionNode deserialize(PDefaultExpansionNode node, RelFromProto s) {
    List<String> path = new ArrayList<>(node.getPathList());
    RelNode input = s.toRel(node.getInput());
    return (DefaultExpansionNode) DefaultExpansionNode.wrap(new NamespaceKey(path), input, input.getRowType(),
      node.getContextSensitive(), Strings.isNullOrEmpty(node.getVersionContext()) ? null : TableVersionContext.deserialize(node.getVersionContext()));
  }
}
