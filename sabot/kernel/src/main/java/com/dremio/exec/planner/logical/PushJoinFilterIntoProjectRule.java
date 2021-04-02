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
package com.dremio.exec.planner.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

/**
 * <pre>
 * Rule to push join filters on literals, past join into the project below, e.g if we have 10 fields below join, 5 on each side:
 * JoinRel(condition=[AND(=($1, $8), =($5, 10))]
 *    ... [left side: $0, $1, $2, $3, $4] ...
 *    ... [right side: $5, $6, $7, $8, $9] ...
 *
 * will be written as:
 *
 * JoinRel(condition=[AND(=($1, $9), =($5, $6))]
 *    ProjectRel($0, $1, $2, $3, $4, $5=[10])
 *    ... [right side: $6, $7, $8, $9, $10] ... every element offset by 1
 *
 * and when left side has 6 fields and right side has 4 fields
 * JoinRel(condition=[AND(=($1, $8), =($5, 10))]
 *    ... [left side: $0, $1, $2, $3, $4, $5] ...
 *    ... [right side: $6, $7, $8, $9] ...
 *
 * will be written as:
 *
 * JoinRel(condition=[AND(=($1, $8), =($5, $10))]
 *    ... [left side: $0, $1, $2, $3, $4, $5] ...
 *    ProjectRel($6, $7, $8, $9, $10=[10])
 *
 * </pre>
 */
public class PushJoinFilterIntoProjectRule extends RelOptRule {

  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PushJoinFilterIntoProjectRule.class);

  public static final RelOptRule INSTANCE = new PushJoinFilterIntoProjectRule(
    operand(JoinRel.class, any()), DremioRelFactories.LOGICAL_BUILDER);

  private final RelBuilderFactory factory;

  private PushJoinFilterIntoProjectRule(RelOptRuleOperand operand, RelBuilderFactory factory) {
    super(operand, "PushJoinFilterIntoProjectRule");
    this.factory = factory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
    List<Boolean> pushedFilters = new ArrayList<>();
    EquiLiteralConditionFinder visitor = new EquiLiteralConditionFinder(true, join.getCluster().getRexBuilder());

    for (RexNode rexNode : joinFilters) {
      pushedFilters.add(rexNode.accept(visitor) != null); // To keep track of which filters should be pushed below
    }

    if (visitor.fields.isEmpty()) {
      return;
    }

    RelBuilder relBuilder = factory.create(join.getCluster(), null);

    // Keeps track of where we add new fields for each constant in a Project below join
    Map<Pair<RexInputRef, RexNode>, Integer> newFieldLocationMap = new HashMap<>();

    // Keeps track of where we add new fields for each constant in a Join
    Set<Integer> newFieldLocationsInJoin = new HashSet<>();

    createJoin(join, join.getLeft(), join.getRight(), relBuilder, joinFilters, visitor.fields, pushedFilters, visitor.filterNulls, newFieldLocationMap, newFieldLocationsInJoin);
    createTopProject(relBuilder, relBuilder.peek().getRowType(), newFieldLocationsInJoin);
    call.transformTo(relBuilder.build());
  }

  private void createTopProject(RelBuilder relBuilder, RelDataType rowType, Set<Integer> newFieldLocationsInJoin) {
    // Add a project on top to filter out the new constant fields we added, to keep the row type same
    List<RexNode> exprs = new ArrayList<>();
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    for (int i = 0; i < fieldList.size(); i++) {
      if (newFieldLocationsInJoin.contains(i)) {
        continue;
      }
      RelDataTypeField field = fieldList.get(i);
      exprs.add(relBuilder.getRexBuilder().makeInputRef(field.getType(), i));
    }
    relBuilder.project(exprs);
  }

  private void createJoin(Join origJoin, RelNode left, RelNode right,
                          RelBuilder relBuilder, List<RexNode> joinFilters,
                          List<Pair<RexInputRef, RexNode>> fields,
                          List<Boolean> pushedFilters, List<Boolean> filterNulls,
                          Map<Pair<RexInputRef, RexNode>, Integer> newFieldLocationMap,
                          Set<Integer> newFieldLocationsInJoin) {

    int origLeftFieldCount = left.getRowType().getFieldCount();
    boolean leftProject = createProjectBelowJoin(relBuilder, left, fields, newFieldLocationMap, origLeftFieldCount, 0, true);
    RelDataType newLeftRowType = relBuilder.peek().getRowType();
    createProjectBelowJoin(relBuilder, right, fields, newFieldLocationMap, origLeftFieldCount, newLeftRowType.getFieldCount(), false);

    int offsetForJoinFilters = 0;
    if (leftProject) {
      // If we added a constant field on left side, we will have to update the new join condition and add offset to all the right fields
      offsetForJoinFilters = newLeftRowType.getFieldCount() - left.getRowType().getFieldCount();
    }

    RexNode newJoinCondition = buildNewJoinCondition(
      origJoin, relBuilder.getRexBuilder(), joinFilters, fields,
      newFieldLocationsInJoin, newFieldLocationMap, pushedFilters, filterNulls, origLeftFieldCount, offsetForJoinFilters);

    relBuilder.join(origJoin.getJoinType(), newJoinCondition);
  }

  private boolean createProjectBelowJoin(RelBuilder relBuilder, RelNode relNode,
                                         List<Pair<RexInputRef, RexNode>> fields,
                                         Map<Pair<RexInputRef, RexNode>, Integer> newFieldLocationMap,
                                         int origLeftFieldCount, int newLeftFieldCount, boolean isLeft) {

    List<RexNode> exprs = new ArrayList<>();
    List<RelDataTypeField> fieldList = relNode.getRowType().getFieldList();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      exprs.add(relBuilder.getRexBuilder().makeInputRef(field.getType(), i));
    }

    boolean noConstantFieldFound = true;
    for (Pair<RexInputRef, RexNode> fieldPair : fields) {
      int index = fieldPair.left.getIndex();
      // If we have $1 = someLiteral condition on a field from left side,
      // push the constant on right side and update the condition with $1 = $n
      // where n is the new index for the constant literal in the project below
      // this join, and vice versa for right side.
      if ((isLeft && index >= origLeftFieldCount) || (!isLeft && index < origLeftFieldCount)) {
        RexNode newRexNode = fieldPair.right;
        newFieldLocationMap.put(fieldPair,
          isLeft ?
            exprs.size() : // If adding the field on left side, no need to offset the new field location, we are adding at the end
            exprs.size() + newLeftFieldCount); // Add offset
        exprs.add(newRexNode);
        noConstantFieldFound = false;
      }
    }

    relBuilder.push(relNode);

    if (noConstantFieldFound) {
      // We didn't add any constant fields. No need to create a project on this side
      return false;
    }

    relBuilder.project(exprs);
    return true;
  }

  private RexNode buildNewJoinCondition(Join origJoin, RexBuilder rexBuilder, List<RexNode> joinFilters,
                                        List<Pair<RexInputRef, RexNode>> fields,
                                        Set<Integer> newFieldLocationsInJoin,
                                        Map<Pair<RexInputRef, RexNode>, Integer> newFieldLocationMap,
                                        List<Boolean> pushedFilters, List<Boolean> filterNulls,
                                        int origLeftFieldCount, int offsetForJoinFilters) {
    List<RexNode> newRexNodes = new ArrayList<>();

    for (int i = 0; i < pushedFilters.size(); i++) {
      // Add all the fields that we have not pushed
      if (!pushedFilters.get(i)) {
        RexNode rexNode = joinFilters.get(i);
        if (offsetForJoinFilters != 0) {
          // If we need to offset right side fields
          newRexNodes.add(rexNode.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
              if (inputRef.getIndex() >= origLeftFieldCount) {
                return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + offsetForJoinFilters);
              }
              return inputRef;
            }
          }));
        } else {
          newRexNodes.add(rexNode);
        }
      }
    }

    for (int i = 0; i < fields.size(); i++) {
      // Add all the fields that we want to update
      Pair<RexInputRef, RexNode> fieldPair = fields.get(i);
      int inputIndex = fieldPair.left.getIndex(); // This is the original index of this input in the original join
      int literalIndex = newFieldLocationMap.get(fieldPair); // This will be the new index of the literal we have pushed below

      RelDataType type = origJoin.getRowType().getFieldList().get(inputIndex).getType();

      // If the original input was on the right side, we need to add the offset
      if (inputIndex >= origLeftFieldCount) {
        // Add the offset we need to add
        inputIndex += offsetForJoinFilters;
      }

      newFieldLocationsInJoin.add(literalIndex);

      SqlBinaryOperator operator = filterNulls.get(i) ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
      RexNode leftInput = rexBuilder.makeInputRef(type, inputIndex);
      RexNode rightInput = rexBuilder.makeInputRef(type, literalIndex);
      newRexNodes.add(rexBuilder.makeCall(operator, leftInput, rightInput));
    }

    return RexUtil.composeConjunction(rexBuilder, newRexNodes, false);
  }

  /**
   * Visitor to traverse a RexNode tree and find if a node is of the type: someVar = someConstant
   * Basic traversal idea copied from org.apache.calcite.plan.RelOptUtil#splitJoinCondition
   */
  private static class EquiLiteralConditionFinder extends RexVisitorImpl<Pair<RexInputRef, RexNode>> {
    private final List<Pair<RexInputRef, RexNode>> fields;
    private final List<Boolean> filterNulls;
    private final RexBuilder rexBuilder;

    protected EquiLiteralConditionFinder(boolean deep, RexBuilder rexBuilder) {
      super(deep);
      this.rexBuilder = rexBuilder;
      this.fields = new ArrayList<>();
      this.filterNulls = new ArrayList<>();
    }

    @Override
    public Pair<RexInputRef, RexNode> visitCall(RexCall call) {
      call = RelOptUtil.collapseExpandedIsNotDistinctFromExpr(call, rexBuilder);
      SqlKind kind = call.getKind();

      // "=" and "IS NOT DISTINCT FROM" are the same except for how they treat nulls.
      if (kind == SqlKind.EQUALS || kind == SqlKind.IS_NOT_DISTINCT_FROM) {
        final List<RexNode> operands = call.getOperands();
        RexInputRef inputRef;
        RexNode literal;

        if (operands.get(0) instanceof RexInputRef &&
          (operands.get(1) instanceof RexLiteral || operands.get(1).getKind() == SqlKind.CAST)) {
          inputRef = (RexInputRef) operands.get(0);
          literal = getLiteralWithOrWithoutCast(operands.get(1));
        } else if (operands.get(1) instanceof RexInputRef &&
          (operands.get(0) instanceof RexLiteral || operands.get(0).getKind() == SqlKind.CAST)) {
          inputRef = (RexInputRef) operands.get(1);
          literal = getLiteralWithOrWithoutCast(operands.get(0));
        } else {
          return null;
        }

        if (literal == null) {
          return null;
        }

        Pair<RexInputRef, RexNode> pair = new Pair<>(inputRef, literal);
        fields.add(pair);
        filterNulls.add(kind == SqlKind.EQUALS);
        return pair;
      }
      return null;
    }

    public RexNode getLiteralWithOrWithoutCast(RexNode rexNode) {
      // This will return a non-null RexNode only if the give node is a literal
      // or a casted literal. Casting can be nested.
      if (rexNode instanceof RexLiteral) {
        return rexNode;
      } else if (rexNode.getKind() == SqlKind.CAST) {
        RexCall call = (RexCall) rexNode;
        if (call.getOperands().size() == 1) {
          RexNode castNode = getLiteralWithOrWithoutCast(call.getOperands().get(0));
          if (castNode != null) {
            return rexNode;
          }
        }
      }
      return null;
    }
  }
}
