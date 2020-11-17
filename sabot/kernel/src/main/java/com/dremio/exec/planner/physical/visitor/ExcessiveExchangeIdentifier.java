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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.google.common.collect.Lists;

public class ExcessiveExchangeIdentifier extends BasePrelVisitor<Prel, ExcessiveExchangeIdentifier.MajorFragmentStat, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcessiveExchangeIdentifier.class);

  private final long targetSliceSize;

  public ExcessiveExchangeIdentifier(long targetSliceSize) {
    this.targetSliceSize = targetSliceSize;
  }

  public static Prel removeExcessiveEchanges(Prel prel, long targetSliceSize) {
    ExcessiveExchangeIdentifier exchange = new ExcessiveExchangeIdentifier(targetSliceSize);
    return prel.accept(exchange, exchange.getNewStat());
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, MajorFragmentStat parent) throws RuntimeException {
    parent.add(prel);
    MajorFragmentStat newFrag = new MajorFragmentStat();
    Prel newChild = ((Prel) prel.getInput()).accept(this, newFrag);

    if (newFrag.isSingular() && parent.isSingular() &&
        // if both of them have soft distribution, we can remove the exchange
        (!newFrag.isDistributionStrict() && !parent.isDistributionStrict())) {
      parent.merge(newFrag);
      //after merge, change to single stream
      RelTraitSet relTraits = newChild.getTraitSet().replace(DistributionTrait.SINGLETON);
      RelNode newSingletonChild = newChild.copy(relTraits, newChild.getInputs());
      return (Prel) newSingletonChild;
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode) newChild));
    }
  }

  @Override
  public Prel visitScreen(ScreenPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScreen(prel);
    RelNode child = ((Prel)prel.getInput()).accept(this, s);
    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(child));
  }

  @Override
  public Prel visitLeaf(LeafPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScan(prel);
    return prel;
  }

  @Override
  public Prel visitPrel(Prel prel, MajorFragmentStat s) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    s.add(prel);

    // Add all children to MajorFragmentStat, before we visit each child.
    // Since MajorFramentStat keeps track of maxCost of Prels in MajorFrag, it's fine to add prel multiple times.
    // Doing this will ensure MajorFragmentStat is same when visit each individual child, in order to make
    // consistent decision.
    for (Prel p : prel) {
      s.add(p);
    }

    for(Prel p : prel) {
      children.add(p.accept(this, s));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  public MajorFragmentStat getNewStat() {
    return new MajorFragmentStat();
  }

  class MajorFragmentStat {
    private DistributionAffinity distributionAffinity = DistributionAffinity.NONE;
    private double maxCost = 0d;
    private int maxWidth = Integer.MAX_VALUE;
    private boolean isMultiSubScan = false;

    public void add(Prel prel) {
      maxCost = Math.max(maxCost, prel.getCostForParallelization());
    }

    public void addScreen(ScreenPrel screenPrel) {
      maxWidth = 1;
      distributionAffinity = screenPrel.getDistributionAffinity();
    }

    public void addScan(LeafPrel prel) {
      maxWidth = Math.min(maxWidth, prel.getMaxParallelizationWidth());
      isMultiSubScan = prel.getMinParallelizationWidth() > 1;
      distributionAffinity = prel.getDistributionAffinity();
      add(prel);
    }

    public boolean isSingular() {
      // do not remove exchanges when a scan has more than one subscans (e.g. SystemTableScan)
      if (isMultiSubScan) {
        return false;
      }

      int suggestedWidth = (int) Math.ceil((maxCost +1)/targetSliceSize);

      int w = Math.min(maxWidth, suggestedWidth);
      if (w < 1) {
        w = 1;
      }
      return w == 1;
    }

    public boolean isDistributionStrict() {
      return distributionAffinity == DistributionAffinity.HARD;
    }

    public void merge(MajorFragmentStat newStat) {
      this.maxCost = Math.max(this.maxCost, newStat.maxCost);
      this.maxWidth = Math.min(this.maxWidth, newStat.maxWidth);
      if (newStat.distributionAffinity == DistributionAffinity.HARD) {
        this.distributionAffinity = DistributionAffinity.HARD;
      }
    }
  }

}
