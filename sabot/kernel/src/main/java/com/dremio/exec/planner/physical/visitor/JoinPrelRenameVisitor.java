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

import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.google.common.collect.Lists;

public class JoinPrelRenameVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private static JoinPrelRenameVisitor INSTANCE = new JoinPrelRenameVisitor();

  public static Prel insertRenameProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);

  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {

    List<RelNode> children = Lists.newArrayList();

    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    final int leftCount = children.get(0).getRowType().getFieldCount();

    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getJoinInput(0, children.get(0));
    RelNode right = prel.getJoinInput(leftCount, children.get(1));

    reNamedChildren.add(left);
    reNamedChildren.add(right);

    return (Prel) prel.copy(prel.getTraitSet(), reNamedChildren);
  }

}
