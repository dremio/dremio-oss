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

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.google.common.collect.Lists;

/**
 * An abstract visitor class that visits each prel and computes cost and width.
 * visitExchange method needs to be implemented to decide how to handle exchange operators.
 */
public abstract class FragmentStatVisitor extends BasePrelVisitor<Prel, FragmentStatVisitor.MajorFragmentStat, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStatVisitor.class);

  private final long targetSliceSize;

  public FragmentStatVisitor(long targetSliceSize) {
    this.targetSliceSize = targetSliceSize;
  }

  @Override
  public abstract Prel visitExchange(ExchangePrel prel, MajorFragmentStat s);

  @Override
  public Prel visitScreen(ScreenPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addScreen(prel);
    RelNode child = ((Prel)prel.getInput()).accept(this, s);
    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(child));
  }

  @Override
  public Prel visitLeaf(LeafPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addLeaf(prel);
    return prel;
  }

  public Prel visitTableFunctionDataScan(TableFunctionPrel prel, MajorFragmentStat s) throws RuntimeException {
    s.addTableFunctionDataScan(prel);
    RelNode child = ((Prel)prel.getInput()).accept(this, s);
    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(child));
  }

  @Override
  public Prel visitPrel(Prel prel, MajorFragmentStat s) throws RuntimeException {
    if (prel instanceof TableFunctionPrel && ((TableFunctionPrel) prel).isDataScan()) {
      return visitTableFunctionDataScan((TableFunctionPrel) prel, s);
    }
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

  final class MajorFragmentStat {
    private DistributionAffinity distributionAffinity;
    private double maxCost;
    private int maxWidth;
    private boolean isMultiSubScan = false;

    public MajorFragmentStat() {
      this(DistributionAffinity.NONE, 0d, Integer.MAX_VALUE);
    }

    private MajorFragmentStat(DistributionAffinity distributionAffinity, double maxCost, int maxWidth) {
      this.distributionAffinity = distributionAffinity;
      this.maxCost = maxCost;
      this.maxWidth = maxWidth;
    }

    public long getTargetSliceSize() {
      return targetSliceSize;
    }

    public int getMaxWidth() {
      return maxWidth;
    }

    public void add(Prel prel) {
      maxCost = Math.max(maxCost, prel.getCostForParallelization());
    }

    public void addScreen(ScreenPrel screenPrel) {
      maxWidth = 1;
      distributionAffinity = screenPrel.getDistributionAffinity();
    }

    public void addLeaf(LeafPrel prel) {
      maxWidth = Math.min(maxWidth, prel.getMaxParallelizationWidth());
      isMultiSubScan = prel.getMinParallelizationWidth() > 1;
      distributionAffinity = prel.getDistributionAffinity();
      add(prel);
    }

    public void addTableFunctionDataScan(TableFunctionPrel prel) {
      int suggestedWidth = (int) Math.ceil(prel.getCluster().getMetadataQuery().getRowCount(prel) / targetSliceSize);
      maxWidth = Math.min(maxWidth, suggestedWidth);
      isMultiSubScan = suggestedWidth > 1;
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
