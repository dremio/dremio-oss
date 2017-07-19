/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.jobs.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.service.job.proto.JoinConditionInfo;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.job.proto.JoinType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Extracts joins and join origins from a rel tree for later use.
 */
public class JoinExtractor {

  /**
   * @return may return null if literals/rexcalls/etc. (not input ref) found.
   */
  private static RelColumnOrigin getOrigin(JoinDefinition join, RexNode operand) {
    int index = getRexInputRefIndex(operand);
    if (index < 0) {
      return null;
    }
    // Note for columns in join conditions that come from subqueries
    // or virtual datasets datasets with filters, a filter may reduce one side
    // of the equality condition to a constant. This null check handles these cases,
    // as one side of the equality condition will lack an origin.
    // Example: select a from t1 join (select a_col from t2 where a_col = 1) sub on t1.a = sub.a_col
    Set<RelColumnOrigin> originsForInputIndex = join.getOrigin().get(index);
    if (originsForInputIndex == null) {
      return null;
    }
    return originsForInputIndex.iterator().next();
  }

  private static int getRexInputRefIndex(RexNode operand) {
    if (SqlKind.INPUT_REF != operand.getKind()) {
      return -1;
    }
    return ((RexInputRef)operand).getIndex();
  }

  public static List<JoinInfo> getJoins(RelNode graph){
    JoinFinder finder = new JoinFinder(graph.getCluster().getMetadataQuery());
    graph.accept(finder);
    List<JoinInfo> definitions = new ArrayList<>();
    joins: for (JoinDefinition joinDefinition : finder.definitions) {
      int degrees = 0; // TODO: implement degrees in extract
      if (joinDefinition.getLeftTables().size() != 1
          || joinDefinition.getRightTables().size() != 1) {
        // this join is not exploitable
        continue;
      }
      List<String> leftTable = joinDefinition.getLeftTables().iterator().next();
      List<String> rightTable = joinDefinition.getRightTables().iterator().next();
      List<JoinConditionInfo> conditions = new ArrayList<>();
      RexNode joinCondition = joinDefinition.getJoinCondition();
      switch (joinCondition.getKind()) {
      // TODO and
      case EQUALS:
        final JoinConditionInfo equalsExtractCond = extractEquals(joinDefinition, (RexCall) joinCondition);
        if (equalsExtractCond == null) {
          continue joins;
        }
        conditions.add(equalsExtractCond);
        break;
      case AND:
        RexCall andCall = (RexCall) joinCondition;
        for (RexNode operand : andCall.getOperands()) {
          if (operand.getKind() == SqlKind.EQUALS) {
            final JoinConditionInfo equalsExtractEquals = extractEquals(joinDefinition, (RexCall) operand);
            if (equalsExtractEquals == null) {
              continue joins;
            }
            conditions.add(equalsExtractEquals);
          } else {
            // this join is not exploitable
            continue joins;
          }
        }
        break;
      default:
        // this join is not exploitable
        continue joins;
      }
      definitions.add(
          new JoinInfo(toJoinType(joinDefinition.getJoinType()), degrees)
          .setLeftTablePathList(leftTable)
          .setRightTablePathList(rightTable)
          .setConditionsList(conditions));
    }
    return definitions;
  }

  private static JoinConditionInfo extractEquals(JoinDefinition joinDefinition, RexCall c) {
    List<RexNode> operands = c.getOperands();
    if (operands.size() != 2) {
      throw new IllegalArgumentException(c.toString());
    }
    RexNode opA = operands.get(0);
    RexNode opB = operands.get(1);
    RelColumnOrigin oA = getOrigin(joinDefinition, opA);
    RelColumnOrigin oB = getOrigin(joinDefinition, opB);

    if (oA == null || oB == null) {
      return null;
    }

    return new JoinConditionInfo(Origins.getColName(oA), Origins.getColName(oB))
        .setTableAList(Origins.getTable(oA))
        .setTableBList(Origins.getTable(oB));
  }

  private static JoinType toJoinType(JoinRelType joinType) {
    switch (joinType) {
    case FULL:
      return JoinType.FullOuter;
    case INNER:
      return JoinType.Inner;
    case LEFT:
      return JoinType.LeftOuter;
    case RIGHT:
      return JoinType.RightOuter;
    default:
      throw new UnsupportedOperationException("Unknown join type " + joinType);
    }
  }

  private static final class JoinFinder extends StatelessRelShuttleImpl {
    private final List<JoinDefinition> definitions = new ArrayList<>();
    private final RelMetadataQuery query;

    public JoinFinder(RelMetadataQuery query) {
      this.query = query;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      RexNode node = join.getCondition();
      Set<List<String>> leftTables = getOriginTables(join.getLeft());
      Set<List<String>> rightTables = getOriginTables(join.getRight());
      InputCollector collector = new InputCollector(join);
      node.accept(collector);
      definitions.add(new JoinDefinition(join.getJoinType(), leftTables, rightTables, node, ImmutableMap.copyOf(collector.origins)));
      return super.visit(join);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof org.apache.calcite.rel.core.Join) {
        org.apache.calcite.rel.core.Join join = (org.apache.calcite.rel.core.Join)other;
        RexNode node = join.getCondition();
        Set<List<String>> leftTables = getOriginTables(join.getLeft());
        Set<List<String>> rightTables = getOriginTables(join.getRight());
        InputCollector collector = new InputCollector(join);
        node.accept(collector);
        definitions.add(new JoinDefinition(join.getJoinType(), leftTables, rightTables, node, ImmutableMap.copyOf(collector.origins)));
      }
      return super.visit(other);
    }

    private Set<List<String>> getOriginTables(RelNode node) {
      Set<List<String>> tables = new HashSet<>();
      for (int i = 0; i < node.getRowType().getFieldCount(); i++) {
        Set<RelColumnOrigin> columnOrigins = query.getColumnOrigins(node, i);
        if (columnOrigins == null) {
          // could not figure out origin
          // join is not exploitable
          continue;
        }
        for (RelColumnOrigin relColumnOrigin : columnOrigins) {
          tables.add(relColumnOrigin.getOriginTable().getQualifiedName());
        }
      }
      return tables;
    }

    private final class InputCollector extends RexShuttle {
      private final Map<Integer, Set<RelColumnOrigin>> origins = Maps.newHashMap();
      private final RelNode join;

      public InputCollector(RelNode join) {
        super();
        this.join = join;
      }

      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        Set<RelColumnOrigin> origin = query.getColumnOrigins(join, inputRef.getIndex());
        if (origin != null && !origin.isEmpty()) {
          origins.put(inputRef.getIndex(), origin);
        }
        return super.visitInputRef(inputRef);
      }
    }
  }

  /**
   * Description of a particular join within a query.
   */
  private static class JoinDefinition {
    /**
     * Type of join
     */
    private final JoinRelType joinType;

    /**
     * root tables on the left of the join
     */
    private final ImmutableSet<List<String>> leftTables;

    /**
     * root tables on the right of the join
     */
    private final ImmutableSet<List<String>> rightTables;

    /**
     * The join condition for this join.
     */
    private final RexNode joinCondition;

    /**
     * Origin information for each RexInputRef in join condition.
     */
    private final ImmutableMap<Integer, Set<RelColumnOrigin>> origin;

    public JoinDefinition(JoinRelType joinType, Set<List<String>> leftTables, Set<List<String>> rightTables, RexNode joinCondition, ImmutableMap<Integer, Set<RelColumnOrigin>> origin) {
      super();
      this.joinType = joinType;
      this.leftTables = ImmutableSet.copyOf(leftTables);
      this.rightTables = ImmutableSet.copyOf(rightTables);
      this.joinCondition = joinCondition;
      this.origin = origin;
    }

    @Override
    public String toString() {
      return "JoinDefinition [joinType=" + joinType + ", joinCondition=" + joinCondition + ", origin=" + origin + "]";
    }

    public JoinRelType getJoinType() {
      return joinType;
    }

    public ImmutableSet<List<String>> getLeftTables() {
      return leftTables;
    }

    public ImmutableSet<List<String>> getRightTables() {
      return rightTables;
    }

    public RexNode getJoinCondition() {
      return joinCondition;
    }

    public ImmutableMap<Integer, Set<RelColumnOrigin>> getOrigin() {
      return origin;
    }
  }
}
