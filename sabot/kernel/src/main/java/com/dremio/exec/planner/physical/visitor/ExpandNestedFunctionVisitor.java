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

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.options.OptionManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunction;

/**
 *
 *
 * <pre>
 * Expand nested functions and push them down to break the nested structure.
 * e.g. an expression like:
 *
 * replace(replace(replace(replace(r_name, 'A', 'B'), 'B', 'C'), 'C', 'D'), 'D', 'E') as foo
 *
 * will have input node:
 * ProjectPrel(foo=[REPLACE(REPLACE(REPLACE(REPLACE($1, 'A':VARCHAR(1), 'B':VARCHAR(1)), 'B':VARCHAR(1), 'C':VARCHAR(1)), 'C':VARCHAR(1), 'D':VARCHAR(1)), 'D':VARCHAR(1), 'E':VARCHAR(1))])
 *
 * And after applying this visitor: (if the depth is set to 1)
 *
 * ProjectPrel($f0=[REPLACE($3, 'D':VARCHAR(1), 'E':VARCHAR(1))])
 *   ProjectPrel(r_regionkey=[$0], r_name=[$1], r_comment=[$2], $f3=[REPLACE($3, 'C':VARCHAR(1), 'D':VARCHAR(1))])
 *     ProjectPrel(r_regionkey=[$0], r_name=[$1], r_comment=[$2], $f3=[REPLACE($3, 'B':VARCHAR(1), 'C':VARCHAR(1))])
 *       ProjectPrel(r_regionkey=[$0], r_name=[$1], r_comment=[$2], $f3=[REPLACE($1, 'A':VARCHAR(1), 'B':VARCHAR(1))])
 * </pre>
 */
public class ExpandNestedFunctionVisitor
    extends BasePrelVisitor<Prel, AtomicBoolean, RuntimeException> {
  private static final String NESTED_EXPRS_PREFIX = "NESTED_EXPRS_";
  private final int maxFunctionDepth;

  private ExpandNestedFunctionVisitor(int maxFunctionDepth) {
    this.maxFunctionDepth = maxFunctionDepth;
  }

  public static Prel pushdownNestedFunctions(Prel prel, OptionManager options) {
    return options.getOption(PlannerSettings.NESTED_FUNCTIONS_PUSHDOWN)
        ? pushdownNestedFunctionsRecursive(
            prel, (int) options.getOption(PlannerSettings.MAX_FUNCTION_DEPTH))
        : prel;
  }

  private static Prel pushdownNestedFunctionsRecursive(Prel prel, int maxFunctionDepth) {
    ExpandNestedFunctionVisitor visitor = new ExpandNestedFunctionVisitor(maxFunctionDepth);
    AtomicBoolean pushedDown = new AtomicBoolean(false);
    do {
      pushedDown.set(false);
      prel = prel.accept(visitor, pushedDown);
    } while (pushedDown.get()); // Recursively push down
    return prel;
  }

  @Override
  public Prel visitPrel(Prel prel, AtomicBoolean value) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();

    for (Prel child : prel) {
      children.add(child.accept(this, value));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitProject(ProjectPrel prel, AtomicBoolean value) throws RuntimeException {
    final RelNode inputRel = prel.getInput();
    final RexBuilder rexBuilder = prel.getCluster().getRexBuilder();
    final RelOptCluster cluster = prel.getCluster();
    final RelDataTypeFactory factory = cluster.getTypeFactory();
    final RelDataType inputRowType = inputRel.getRowType();
    final List<RexNode> projects = new ArrayList<>();
    final List<RexNode> nestedNodes = new ArrayList<>();
    final NestedFunctionFinder finder =
        new NestedFunctionFinder(rexBuilder, maxFunctionDepth, inputRowType.getFieldCount());

    for (RexNode rexNode : prel.getProjects()) {
      projects.add(rexNode.accept(finder));
      if (finder.isNested()) {
        nestedNodes.addAll(finder.getPushedDownExprs());
      }
      finder
          .reset(); // Reset so we can reuse the same instance which is keeping track of input count
      // when an expression is pushed down.
    }
    if (nestedNodes.isEmpty()) {
      return super.visitProject(prel, value);
    }

    value.set(true);

    final List<RexNode> pushedDownNodes = new ArrayList<>();
    final List<RelDataType> bottomProjectType = new ArrayList<>();
    final List<String> fieldNameList = new ArrayList<>();
    for (int i = 0; i < inputRowType.getFieldCount(); i++) {
      final RelDataTypeField field = inputRowType.getFieldList().get(i);
      final RelDataType type = field.getType();
      fieldNameList.add(field.getName());
      bottomProjectType.add(type);
      pushedDownNodes.add(rexBuilder.makeInputRef(inputRowType.getFieldList().get(i).getType(), i));
    }

    for (int i = 0; i < nestedNodes.size(); i++) {
      RexNode rexNode = nestedNodes.get(i);
      pushedDownNodes.add(rexNode);
      fieldNameList.add(NESTED_EXPRS_PREFIX + i);
      bottomProjectType.add(rexNode.getType());
    }

    final RelDataType bottomType = factory.createStructType(bottomProjectType, fieldNameList);
    final ProjectPrel bottomProject =
        ProjectPrel.create(cluster, prel.getTraitSet(), inputRel, pushedDownNodes, bottomType);

    final List<RelDataType> topProjectType =
        projects.stream().map(RexNode::getType).collect(Collectors.toList());
    final RelDataType topType =
        factory.createStructType(topProjectType, prel.getRowType().getFieldNames());
    return ProjectPrel.create(cluster, prel.getTraitSet(), bottomProject, projects, topType);
  }

  public static final class NestedFunctionFinder extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final int maxFunctionDepth;

    // Input size is needed because we add the pushed down expression at the end of the input node
    private int inputSize;

    // We found nesting.
    private boolean isNested;

    // Keep a list of nested function. There can be more than one like: replace(replace..) and
    // replace(replace..)
    private List<RexNode> pushedDownExprs;

    // Use a map to keep track of duplicate expressions like: replace(replace(..)) or
    // replace(replace(..))
    private Map<RexNode, RexNode> dupExprMap;

    private int functionDepth;

    public NestedFunctionFinder(RexBuilder rexBuilder, int maxFunctionDepth, int inputSize) {
      this.rexBuilder = rexBuilder;
      this.maxFunctionDepth = maxFunctionDepth;
      this.inputSize = inputSize;
      this.functionDepth = 0;
      this.reset();
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getOperator() instanceof SqlFunction) {
        functionDepth++;
        final RexNode rexNode;
        if (functionDepth > maxFunctionDepth) {
          // If we have reached the max function depth, break here and push the nested expression
          // down
          if (dupExprMap.containsKey(call)) {
            rexNode = dupExprMap.get(call);
          } else {
            rexNode =
                rexBuilder.makeInputRef(
                    call.getType(), inputSize++); // Make a reference of pushed down expression
            isNested = true;
            pushedDownExprs.add(call); // This expression is pushed down now
            dupExprMap.put(call, rexNode);
          }
        } else {
          rexNode = super.visitCall(call);
        }
        functionDepth--;
        return rexNode;
      }
      return super.visitCall(call);
    }

    public void reset() {
      this.isNested = false;
      this.pushedDownExprs = new ArrayList<>();
      this.dupExprMap = new HashMap<>();
      this.functionDepth = 0;
    }

    public boolean isNested() {
      return isNested;
    }

    public List<RexNode> getPushedDownExprs() {
      return pushedDownExprs;
    }
  }
}
