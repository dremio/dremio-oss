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

import static com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils.STRUCTURED_WRAPPER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * Rule that pushes project past a flatten.
 *
 * For example, consider the following query with flatten:
 * select sub.zflat.orange, sub.lflat from (select flatten(t.z) as zflat, flatten(t.l) as lflat from dfs_test.tmp.parquetTable t) sub
 *
 * In this case, we want all of "flatten(t.l), but the projection sub.zflat.orange indicates that we only want one particular column/field within zflat.
 * So, when reading data from parquet file, we should only read all of t.l and only t.z.orange from t.z.  This rule pushes the project past flatten
 * so that we can push the column selection into the scan.
 *
 * ProjectForFlattenRel is a project (with an additional field keeping track of flatten expressions).  ProjectForFlattenRel
 * always have infinite cost, and will not be part of the plan unless pushed into scan.
 */
public class PushProjectPastFlattenRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushProjectPastFlattenRule();

  private PushProjectPastFlattenRule() {
    super(RelOptHelper.some(ProjectRel.class, RelOptHelper.any(FlattenRel.class)), "PushProjectPastFlattenRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ProjectRel project = call.rel(0);

    if (project.hasComplexFields() || !project.canPushPastFlatten()) {
      return false;
    }

    final FlattenRel flatten = call.rel(1);
    if(flatten.getNumProjectsPushed() > 7){
      return false;
    }

    return true;
  }

  private static int findStructuredColumnInputRefIndex(RexNode rexNode) {
    if (rexNode == null) {
      return -1;
    }

    if (rexNode instanceof RexInputRef) {
      return ((RexInputRef) rexNode).getIndex();
    }

    if ((rexNode instanceof RexFieldAccess)) {
      return findStructuredColumnInputRefIndex(((RexFieldAccess) rexNode).getReferenceExpr());
    }

    if (rexNode instanceof RexCall) {
      String functionName = ((RexCall) rexNode).getOperator().getName();
      if (functionName.equalsIgnoreCase("item")) {
        return findStructuredColumnInputRefIndex(((RexCall) rexNode).getOperands().get(0));
      } else if (functionName.equalsIgnoreCase(STRUCTURED_WRAPPER.getName())) {
        return findStructuredColumnInputRefIndex(((RexCall) rexNode).getOperands().get(0));
      }
    }

    return -1;
  }

  private static RexNode replaceStructuredColumnInputRefIndex(RexBuilder rexBuilder, RexNode rexNode, int orig, int replace) {
    if (rexNode == null) {
      return null;
    }

    if (rexNode instanceof RexInputRef) {
      assert ((RexInputRef) rexNode).getIndex() == orig;
      return rexBuilder.makeInputRef(rexNode.getType(), replace);
    }

    if (rexNode instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
      RexNode newExpr = replaceStructuredColumnInputRefIndex(rexBuilder, fieldAccess.getReferenceExpr(), orig, replace);
      return rexBuilder.makeFieldAccess(newExpr, fieldAccess.getField().getName(), true);
    }

    if (rexNode instanceof RexCall) {
      String functionName = ((RexCall) rexNode).getOperator().getName();
      if (functionName.equalsIgnoreCase("item")) {
        assert ((RexCall) rexNode).getOperands().size() == 2;
        RexNode newInput0 = replaceStructuredColumnInputRefIndex(rexBuilder, ((RexCall) rexNode).getOperands().get(0), orig, replace);
        RexNode newInput1 = replaceStructuredColumnInputRefIndex(rexBuilder, ((RexCall) rexNode).getOperands().get(1), orig, replace);
        return rexBuilder.makeCall(((RexCall) rexNode).getOperator(), newInput0, newInput1);
      } else if (functionName.equalsIgnoreCase(STRUCTURED_WRAPPER.getName())) {
        assert ((RexCall) rexNode).getOperands().size() == 1;
        return replaceStructuredColumnInputRefIndex(rexBuilder, ((RexCall) rexNode).getOperands().get(0), orig, replace);
      }
    }

    return rexNode;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = call.rel(0);
    final FlattenRel flatten = call.rel(1);
    RelNode newProjectRel = getProjectForFlattenParam(project, flatten);
    if (newProjectRel != null) {
      call.transformTo(newProjectRel);
    }
  }

  private void getItemAndProjExprs(ProjectRel project,
                                   FlattenRel flatten,
                                   Map<Integer, List<RexNode>> itemExprs,
                                   Set<Integer> projExprs) {
    // Get a map<input ref index, list of expressions related to this input ref>
    for (RexNode flattenRexNode : flatten.getToFlatten()) {
      // prune out the flatten operator item, if there are any
      assert flattenRexNode instanceof RexInputRef;
      int flattenIndex = ((RexInputRef) flattenRexNode).getIndex();
      for (RexNode projExpr : project.getProjects()) {
        int projectIndex;
        if (projExpr instanceof RexCall || projExpr instanceof RexFieldAccess) {
          projectIndex = findStructuredColumnInputRefIndex(projExpr);
        } else if (projExpr instanceof RexInputRef) {
          projectIndex = ((RexInputRef) projExpr).getIndex();
        } else {
          throw new RuntimeException("Cannot push complex project past flatten, " + project);
        }

        if (projectIndex == flattenIndex) {
          List<RexNode> listOfSelections = itemExprs.get(flattenIndex);
          if (listOfSelections == null) {
            listOfSelections = new ArrayList<>();
          }
          listOfSelections.add(projExpr);
          itemExprs.put(flattenIndex, listOfSelections);
        }
        projExprs.add(projectIndex);
      }

      if (itemExprs.get(flattenIndex) == null) {
        List<RexNode> newList = new ArrayList<>(1);
        newList.add(flattenRexNode);
        itemExprs.put(flattenIndex, newList);
        projExprs.add(flattenIndex);
      }
    }
  }

  private RelNode getProjectForFlattenParam(ProjectRel project, FlattenRel flatten) {

    // flattenIndex -> expressions in project that are referencing the flattened column
    Map<Integer, List<RexNode>> itemExprs = new HashMap<>();
    // Set of columns referenced in the project
    Set<Integer> projExprs = new HashSet<>();
    getItemAndProjExprs(project, flatten, itemExprs, projExprs);

    // If flatten related expressions are empty, then we shouldn't transform.
    if (itemExprs == null || itemExprs.size() == 0) {
      return null;
    }

    RelDataTypeFactory factory = project.getCluster().getTypeFactory();
    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    List<RelDataTypeField> flattenInputRow = flatten.getInput().getRowType().getFieldList();

    List<RexNode> rexProjExprs = new ArrayList<>();
    List<RexNode> rexItemExprs = new ArrayList<>();
    List<RelDataType> projDataTypes = new ArrayList<>();
    List<String> projFieldNames = new ArrayList<>();
    List<RexInputRef> newFlatten = new ArrayList<>(flatten.getToFlatten().size());

    Map<Integer, Integer> oldProjIndexToNewIndex = new HashMap<>();
    for (int index = 0; index < flattenInputRow.size(); index++) {
      if (projExprs.contains(index)) {
        rexProjExprs.add(rexBuilder.makeInputRef(flattenInputRow.get(index).getType(), index));
        projDataTypes.add(flattenInputRow.get(index).getType());
        projFieldNames.add(flattenInputRow.get(index).getName());
        oldProjIndexToNewIndex.put(index, projDataTypes.size()-1);
        if (itemExprs.get(index) != null) {
          rexItemExprs.addAll(itemExprs.get(index));
          newFlatten.add(rexBuilder.makeInputRef(projDataTypes.get(projDataTypes.size()-1), projDataTypes.size()-1));
        } else {
          rexItemExprs.add(rexBuilder.makeInputRef(flattenInputRow.get(index).getType(), index));
        }
      }
    }

    // Convert the old map<input ref, list of expressions> to refer to the updated input ref (with the project pushed down)
    List<RexNode> newProjectRelExprs = new ArrayList<>(project.getProjects().size());
    int index = 0;
    for (RexNode expr : project.getProjects()) {
      if (expr instanceof RexInputRef) {
        int oldIndex = ((RexInputRef) expr).getIndex();
        newProjectRelExprs.add(
            rexBuilder.makeInputRef(project.getRowType().getFieldList().get(index).getType(),
                oldProjIndexToNewIndex.get(oldIndex)));
      } else if (expr instanceof RexCall || expr instanceof RexFieldAccess){
        int oldIndex = findStructuredColumnInputRefIndex(expr);
        RexNode replaced = replaceStructuredColumnInputRefIndex(rexBuilder, expr, oldIndex, oldProjIndexToNewIndex.get(oldIndex));
        assert replaced != null;
        newProjectRelExprs.add(replaced);
      }
      index++;
    }

    ProjectForFlattenRel newInput = new ProjectForFlattenRel(
        project.getCluster(),
        project.getTraitSet(),
        flatten.getInput(),
        factory.createStructType(projDataTypes, projFieldNames),
        rexProjExprs,
        rexItemExprs);
    FlattenRel newFlattenRel = new FlattenRel(
        flatten.getCluster(),
        flatten.getTraitSet(),
        newInput,
        newFlatten,
        flatten.getNumProjectsPushed() + 1);
    ProjectRel newProjectRel = ProjectRel.create(
        project.getCluster(),
        project.getTraitSet(),
        newFlattenRel,
        newProjectRelExprs,
        project.getRowType(),
        false);

    return newProjectRel;

  }
}
