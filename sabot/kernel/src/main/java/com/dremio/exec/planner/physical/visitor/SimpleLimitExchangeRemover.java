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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.TopNPrel;
import com.dremio.exec.planner.physical.WindowPrel;

/**
 * Removes all exchanges if the plan fits the following description:
 *   Leaf limits are disabled.
 *   Plan has no joins, window operators or aggregates (union alls are okay)
 *   Plan has at least one subpattern that is scan > project > limit or scan > limit,
 *   The limit is slice target or less
 *   All scans are soft affinity
 */
public class SimpleLimitExchangeRemover {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleLimitExchangeRemover.class);

  public static Prel apply(PlannerSettings settings, Prel input){
    if(!settings.isTrivialSingularOptimized() || settings.isLeafLimitsEnabled()) {
      return input;
    }

    if(input.accept(new Identifier(), false)){
      return input.accept(new AllExchangeRemover(), null);
    }
    return input;
  }

  private static class Identifier extends BasePrelVisitor<Boolean, Boolean, RuntimeException> {

    @Override
    public Boolean visitPrel(Prel prel, Boolean isTrivial) {
      if(prel instanceof WindowPrel || prel instanceof JoinPrel || prel instanceof AggPrelBase || prel instanceof SortPrel || prel instanceof TopNPrel || prel instanceof FilterPrel){
        return false;
      }

      if(prel instanceof LimitPrel){
        if(!((LimitPrel) prel).isTrivial()){
          return false;
        }

        isTrivial = true;
      }

      Boolean okay = true;
      for(Prel p : prel){
        okay = okay && p.accept(this, isTrivial);
      }

      return okay;
    }

    @Override
    public Boolean visitLeaf(LeafPrel prel, Boolean isTrivial) {
      if(prel.getDistributionAffinity() == DistributionAffinity.HARD){
        return false;
      }

      return isTrivial;
    }


  }

  private static class AllExchangeRemover extends BasePrelVisitor<Prel, Void, RuntimeException> {
    @Override
    public Prel visitExchange(ExchangePrel prel, Void dummy) {
      return (Prel) ((Prel)prel.getInput()).accept(this, null);
    }

    @Override
    public Prel visitPrel(Prel prel, Void dummy) {
      List<RelNode> children = new ArrayList<>();
      for(Prel p : prel){
        children.add(p.accept(this, null));
      }
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }

  }

}
