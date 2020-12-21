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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.options.OptionManager;

/**
 * A Reflection Materialization Stripping Organizer.
 */
public class StrippingFactory {

  private static final String NODE_STRIPPER = "dremio.reflection.planning.node-stripper.class";
  private static final PassThruNodeStripper NO_OP_STRIPPER = new PassThruNodeStripper();

  public static final int NO_STRIP_VERSION = 0;
  public static final int RETAIN_EXPANSION_NODE_STRIP_VERSION = 2;
  public static final int LATEST_STRIP_VERSION = 2;

  private final OptionManager options;
  private final SabotConfig config;

  public StrippingFactory(OptionManager options, SabotConfig config) {
    super();
    this.options = options;
    this.config = config;
  }

  public StripResult strip(RelNode query, ReflectionType type, boolean isIncremental, int stripVersion) {
    NodeStripper stripper = type == ReflectionType.EXTERNAL ? new PassThruNodeStripper() : config.getInstance(NODE_STRIPPER, NodeStripper.class, NO_OP_STRIPPER);
    return stripper.apply(options, type, query, isIncremental, stripVersion);
  }

  /**
   * Return a strip result with no stripping applied.
   * @param node
   * @return
   */
  public static StripResult noStrip(RelNode node) {
    return new StripResult(ExpansionNode.removeFromTree(node), new StripLeaf(node.getCluster(), node.getCluster().traitSet(), node.getRowType()));
  }

  /**
   * A normalizer class doing nothing
   */
  private static final class PassThruNodeStripper implements NodeStripper {

    @Override
    public StripResult apply(OptionManager options, ReflectionType reflectionType, RelNode node, boolean isIncremental, int stripVersion) {
      return noStrip(node);
    }

  }

  public interface NodeStripper {
    StripResult apply(OptionManager options, ReflectionType reflectionType, RelNode node, boolean isIncremental, int stripVersion);
  }

  public static class StripResult {
    private final RelNode normalized;
    private final RelNode stripFragment;

    public StripResult(RelNode normalized, RelNode stripFragment) {
      super();
      this.normalized = normalized;
      this.stripFragment = stripFragment;
    }

    public RelNode getNormalized() {
      return normalized;
    }

    public RelNode applyStrippedNodes(RelNode belowStrippedNode) {
      return stripFragment.accept(new RelShuttleImpl() {

        @Override
        public RelNode visit(RelNode other) {
          if(other instanceof StripLeaf) {
            return belowStrippedNode;
          }
          return super.visit(other);
        }

      });
    }

    public StripResult transformNormalized(RelTransformer transformer) {
      return new StripResult(transformer.transform(normalized), stripFragment);
    }

  }

  /**
   * The leaf of a strip set. The strip set is at the top of the tree and should be placed above the materialization.
   */
  public static class StripLeaf extends AbstractRelNode {

    public StripLeaf(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
      super(cluster, traitSet);
      this.rowType = rowType;
    }

  }
}
