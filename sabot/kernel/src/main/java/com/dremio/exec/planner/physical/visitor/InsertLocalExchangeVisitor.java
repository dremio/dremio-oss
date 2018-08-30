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

import java.util.Collections;
import java.util.List;


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.HashPrelUtil.HashExpressionCreatorHelper;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.UnorderedDeMuxExchangePrel;
import com.dremio.exec.planner.physical.UnorderedMuxExchangePrel;
import com.dremio.exec.planner.sql.SqlOperatorImpl;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private final boolean isMuxEnabled;
  private final boolean isDeMuxEnabled;


  public static class RexNodeBasedHashExpressionCreatorHelper implements HashExpressionCreatorHelper<RexNode> {
    private final RexBuilder rexBuilder;

    public RexNodeBasedHashExpressionCreatorHelper(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode createCall(String funcName, List<RexNode> inputFields) {
      final SqlOperatorImpl op =
          new SqlOperatorImpl(funcName, inputFields.size(), true);
      return rexBuilder.makeCall(op, inputFields);
    }
  }

  public static Prel insertLocalExchanges(Prel prel, OptionManager options) {
    final boolean isMuxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE);
    final boolean isDeMuxEnabled = options.getOption(PlannerSettings.DEMUX_EXCHANGE);

    if (isMuxEnabled || isDeMuxEnabled) {
      return prel.accept(new InsertLocalExchangeVisitor(isMuxEnabled, isDeMuxEnabled), null);
    }

    return prel;
  }

  public InsertLocalExchangeVisitor(boolean isMuxEnabled, boolean isDeMuxEnabled) {
    this.isMuxEnabled = isMuxEnabled;
    this.isDeMuxEnabled = isDeMuxEnabled;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getInput()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel
    //   If MuxExchange is enabled, insert a UnorderedMuxExchangePrel before HashToRandomExchangePrel.
    //   If DeMuxExchange is enabled, insert a UnorderedDeMuxExchangePrel after HashToRandomExchangePrel.
    if (!(prel instanceof HashToRandomExchangePrel)) {
      return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
    }

    Prel newPrel = child;

    if (isMuxEnabled) {
      newPrel = new UnorderedMuxExchangePrel(child.getCluster(), child.getTraitSet(), child);
    }

    newPrel = new HashToRandomExchangePrel(prel.getCluster(),
        prel.getTraitSet(), newPrel, ((HashToRandomExchangePrel) prel).getFields());

    if (isDeMuxEnabled) {
      HashToRandomExchangePrel hashExchangePrel = (HashToRandomExchangePrel) newPrel;
      // Insert a DeMuxExchange to narrow down the number of receivers
      newPrel = new UnorderedDeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), hashExchangePrel,
          hashExchangePrel.getFields());
    }

    return newPrel;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
