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
package com.dremio.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.common.types.TypeProtos;
import com.dremio.exec.planner.physical.EmptyPrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.UnionPrel;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;

/**
 * Removes a node and propagates the EmptyPrel up if the node under this is an EmptyPrel, for following nodes:
 * - Filter
 * - Project
 * - Join
 * - Union
 */
public class EmptyPrelPropagator extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private final SqlHandlerConfig config;

  private EmptyPrelPropagator(SqlHandlerConfig config) {
    this.config = config;
  }

  public static Prel propagateEmptyPrel(SqlHandlerConfig config, Prel prel) {
    return prel.accept(new EmptyPrelPropagator(config), null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();
    for (Prel child : prel) {
      child = child.accept(this, value);
      children.add(child);
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitFilter(FilterPrel filter, Void value) throws RuntimeException {
    RelNode child = ((Prel) filter.getInput()).accept(this, value);
    if (child instanceof EmptyPrel) {
      return createEmptyPrelFromNode(filter);
    } else {
      return (FilterPrel) filter.copy(filter.getTraitSet(), ImmutableList.of(child));
    }
  }

  @Override
  public Prel visitJoin(JoinPrel join, Void value) throws RuntimeException {
    JoinPrel newJoin = (JoinPrel) visitPrel(join, value);
    JoinRelType joinType = newJoin.getJoinType();
    RelNode right = newJoin.getRight();
    RelNode left = newJoin.getLeft();
    boolean createEmptyPrelFromNode = (right instanceof EmptyPrel && left instanceof EmptyPrel) || // Both nodes are empty
      (joinType == JoinRelType.INNER && (right instanceof EmptyPrel || left instanceof EmptyPrel)) || // Inner join and either node is empty
      (joinType == JoinRelType.LEFT && left instanceof EmptyPrel) || // Left join and left node is empty
      (joinType == JoinRelType.RIGHT && right instanceof EmptyPrel); // Right join and right node is empty
    if (createEmptyPrelFromNode) {
      return createEmptyPrelFromNode(newJoin);
    } else {
      return newJoin;
    }
  }

  @Override
  public Prel visitUnion(UnionPrel union, Void value) throws RuntimeException {
    UnionPrel newUnion = (UnionPrel) visitPrel(union, value);
    for (Prel child : newUnion) {
      if (!(child instanceof EmptyPrel)) {
        return newUnion;
      }
    }
    return createEmptyPrelFromNode(newUnion);
  }

  @Override
  public Prel visitProject(ProjectPrel project, Void value) throws RuntimeException {
    RelNode child = ((Prel) project.getInput(0)).accept(this, value);
    if (child instanceof EmptyPrel) {
      return createEmptyPrelFromNode(project);
    } else {
      return (ProjectPrel) project.copy(project.getTraitSet(), ImmutableList.of(child));
    }
  }

  private Prel createEmptyPrelFromNode(Prel prel) {
    for (RelDataTypeField field : prel.getRowType().getFieldList()) {
      if (TypeInferenceUtils.getMinorTypeFromCalciteType(field.getValue()) == TypeProtos.MinorType.LATE) {
        return prel;
      }
    }
    try {
      BatchSchema batchSchema = PrelTransformer.convertToPop(config, prel)
        .getProps().getSchema().clone(BatchSchema.SelectionVectorMode.NONE);
      return new EmptyPrel(prel.getCluster(), prel.getTraitSet(), prel.getRowType(), batchSchema);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
