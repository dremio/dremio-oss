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
package com.dremio.extra.exec.store.dfs.parquet.pushdownfilter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

/**
 * Extract a sub filter form tree such that the resulting filter will return the minimal super set
 * of results for the input filter.
 */
public class FilterExtractor {
  private final RexBuilder rexBuilder;
  private final Predicate<RexNode> leafAcceptor;

  private FilterExtractor(RexBuilder rexBuilder, Predicate<RexNode> leafAcceptor) {
    this.rexBuilder = rexBuilder;
    this.leafAcceptor = leafAcceptor;
  }

  private RexNode extract(RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      switch (rexCall.getOperator().getKind()) {
        case AND: {
          boolean changed = false;
          List<RexNode> nodeList = new ArrayList<>();
          for (RexNode sub : rexCall.getOperands()) {
            RexNode subRewritten = extract(sub);
            changed |= subRewritten != sub;
            if (!subRewritten.isAlwaysTrue()) {
              nodeList.add(subRewritten);
            }
          }
          if (changed) {
            return RexUtil.composeConjunction(rexBuilder, nodeList, false);
          } else {
            return rexCall;
          }

        }
        case OR: {
          boolean changed = false;
          List<RexNode> nodeList = new ArrayList<>();
          for (RexNode sub : rexCall.getOperands()) {
            RexNode subRewritten = extract(sub);
            changed |= subRewritten != sub;
            if (subRewritten.isAlwaysTrue()) {
              return rexBuilder.makeLiteral(true);
            } else {
              nodeList.add(subRewritten);
            }
          }
          if (changed) {
            return RexUtil.composeDisjunction(rexBuilder, nodeList, false);
          } else {
            return rexCall;
          }
        }
      }
    }

    if (leafAcceptor.test(rexNode)) {
      return rexNode;
    } else {
      return rexBuilder.makeLiteral(true);
    }
  }

  public static RexNode extractFilter(
      RexBuilder rexBuilder,
      RexNode filter,
      Predicate<RexNode> filterPredicate) {
    FilterExtractor filterExtractor = new FilterExtractor(rexBuilder, filterPredicate);
    return filterExtractor.extract(filter);
  }

}
