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
package com.dremio.sabot.op.fromjson;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.LAST_MATCHING_MAP_ENTRY_FOR_KEY;
import static com.dremio.sabot.op.fromjson.ComplexFunctionPushDownVisitor.Pair.NO_OP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.dremio.exec.planner.common.MoreRexUtil;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Push down complex functions to the Project below.
 */
public abstract class ComplexFunctionPushDownVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  abstract RexShuttle getReplaceShuttle(RexBuilder rexBuilder, int fieldCount, List<RexNode> pushdownList);

  abstract Function<SqlOperator, Boolean> getPredicate();

  abstract String getFieldName();

  /**
   * Push down all the convert_fromjson in the project below to avoid
   * creating multiple ConvertFromJsonPrel nodes.
   */
  private static final ComplexFunctionPushDownVisitor CONVERT_FROM_PUSHDOWN = new ComplexFunctionPushDownVisitor() {
    @Override
    RexShuttle getReplaceShuttle(RexBuilder rexBuilder, int fieldCount,
                                 List<RexNode> pushdownList) {
      return new ConvertFromJsonReplacer(rexBuilder, fieldCount, pushdownList);
    }

    @Override
    Function<SqlOperator, Boolean> getPredicate() {
      return op -> op.getName().equalsIgnoreCase("convert_fromjson");
    }

    @Override
    String getFieldName() {
      return "CONVERT_FROM_JSON";
    }
  };

  /**
   * <pre>
   * Push down arguments in field access functions because we only support
   * execution of a reference to an RexInputRef, so e.g.
   * Project [ ... ITEM(foo(...), 'id') ... ] is not supported, so we rewrite it to:
   *
   * Project [ ... ITEM($x, 'id) ... ]
   *   Project [ ... , $x = foo(...) ]
   * </pre>
   */
  private static final ComplexFunctionPushDownVisitor FIELD_ACCESS_FUNCTIONS_PUSHDOWN = new ComplexFunctionPushDownVisitor() {
    @Override
    RexShuttle getReplaceShuttle(RexBuilder rexBuilder, int fieldCount,
                                 List<RexNode> pushdownList) {
      return new FieldAccessFunctionReplacer(rexBuilder, fieldCount, pushdownList);
    }

    @Override
    Function<SqlOperator, Boolean> getPredicate() {
      return op -> op == SqlStdOperatorTable.ITEM || op == SqlStdOperatorTable.DOT;
    }

    @Override
    String getFieldName() {
      return "FIELD_ACCESS_EXPR";
    }
  };

  public static Prel convertFromJsonPushDown(Prel prel, OptionManager options) {
    if (options.getOption(PlannerSettings.CONVERT_FROM_JSON_PUSHDOWN)) {
      // Push down convert_fromJson
      prel = prel.accept(CONVERT_FROM_PUSHDOWN, null);
    }
    if (options.getOption(PlannerSettings.FIELD_ACCESS_FUNCTIONS_PUSHDOWN)) {
      // Push down field access functions
      prel = prel.accept(FIELD_ACCESS_FUNCTIONS_PUSHDOWN, null);
    }
    return prel;
  }

  @Override
  public Prel visitPrel(Prel prel, Void voidValue) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();

    for (Prel child : prel) {
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel join, Void voidValue) throws RuntimeException {
    if (MoreRexUtil.hasFunction(join.getCondition(), getPredicate())) {
      throw new UnsupportedOperationException("Unsupported convert_fromJson found in a Join condition.");
    } else {
      return visitPrel(join, voidValue);
    }
  }

  @Override
  public Prel visitProject(ProjectPrel project, Void voidValue) throws RuntimeException {
    final boolean foundConvertFrom = project.getChildExps().stream().anyMatch(rexNode -> MoreRexUtil.hasFunction(rexNode, getPredicate()));
    if (foundConvertFrom) {
      final List<RexNode> expressions = project.getProjects();
      Pair bottomProjAndTopExprs = pushdownConvertFrom(
        project,
        ((Prel) project.getInput()).accept(this, null),
        expressions);

      if (bottomProjAndTopExprs == NO_OP) {
        return project;
      }

      final ProjectPrel bottomProject = bottomProjAndTopExprs.projectPrel;
      final List<RexNode> topExprs = bottomProjAndTopExprs.replacedExprs;

      final RelOptCluster cluster = project.getCluster();
      final RelDataTypeFactory factory = cluster.getTypeFactory();

      final List<RelDataType> topProjectType = topExprs.stream().map(RexNode::getType).collect(Collectors.toList());
      final RelDataType topType = factory.createStructType(topProjectType, project.getRowType().getFieldNames());
      // Top project which references to convert_fromjson fields in the project below.
      return ProjectPrel.create(cluster, project.getTraitSet(), bottomProject, topExprs, topType);
    } else {
      return visitPrel(project, voidValue);
    }
  }

  @Override
  public Prel visitFilter(FilterPrel filter, Void voidValue) throws RuntimeException {
    if (MoreRexUtil.hasFunction(filter.getCondition(), getPredicate())) {
      Pair bottomProjAndTopExprs = pushdownConvertFrom(
        filter,
        ((Prel) filter.getInput()).accept(this, null),
        Collections.singletonList(filter.getCondition()));

      if (bottomProjAndTopExprs == NO_OP) {
        return filter;
      }

      final ProjectPrel bottomProject = bottomProjAndTopExprs.projectPrel;
      final RexNode newFilterCondition = bottomProjAndTopExprs.replacedExprs.get(0); // There should only be one rex node
      return (Prel) filter.copy(filter.getTraitSet(), bottomProject, newFilterCondition);
    } else {
      return visitPrel(filter, voidValue);
    }
  }

  private Pair pushdownConvertFrom(Prel parent, Prel child, List<RexNode> expressions) {
    final RelDataType inputRowType = child.getRowType();
    final List<RexNode> pushdownList = new ArrayList<>();
    final List<RexNode> topExprs = new ArrayList<>();
    final RexShuttle replacer = getReplaceShuttle(parent.getCluster().getRexBuilder(),
      inputRowType.getFieldCount(), pushdownList);
    expressions.forEach(rexNode -> topExprs.add(rexNode.accept(replacer)));

    if (pushdownList.isEmpty()) {
      // Nothing pushed down. Return.
      return NO_OP;
    }
    // Push all the nodes in the project below.
    final RelOptCluster cluster = parent.getCluster();
    final RelDataTypeFactory factory = cluster.getTypeFactory();

    final List<RexNode> bottomExprs = new ArrayList<>();
    final List<RelDataType> bottomProjectType = new ArrayList<>();
    final List<String> fieldNameList = new ArrayList<>();

    for (int i = 0; i < inputRowType.getFieldCount(); i++) {
      final RelDataTypeField field = inputRowType.getFieldList().get(i);
      final RelDataType type = field.getType();
      fieldNameList.add(field.getName());
      bottomProjectType.add(type);
      bottomExprs.add(parent.getCluster().getRexBuilder().makeInputRef(type, i));
    }

    for (RexNode rexNode : pushdownList) {
      bottomExprs.add(rexNode);
      fieldNameList.add(getFieldName()); // We'll uniquify these names
      bottomProjectType.add(rexNode.getType());
    }

    final RelDataType bottomType = factory.createStructType(bottomProjectType,
      SqlValidatorUtil.uniquify(
        fieldNameList,
        SqlValidatorUtil.EXPR_SUGGESTER,
        factory.getTypeSystem().isSchemaCaseSensitive()));
    // Bottom project with pushed down nodes appended at the end
    final ProjectPrel bottomProject = ProjectPrel.create(cluster, parent.getTraitSet(), child, bottomExprs, bottomType);

    return new Pair(bottomProject, topExprs);
  }

  /**
   * Finds convert_fromjson nodes and replaces them with their reference
   * in the project below.
   */
  private static final class ConvertFromJsonReplacer extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final int fieldCount;
    private final List<RexNode> convertFromJsonList;
    private final Map<Integer, Integer> convertFromJsonIndexMap;

    public ConvertFromJsonReplacer(RexBuilder rexBuilder, int fieldCount,
                                   List<RexNode> convertFromJsonList) {
      this.rexBuilder = rexBuilder;
      this.fieldCount = fieldCount; // Used for shift index because we push down at the end of the Project below
      this.convertFromJsonList = convertFromJsonList;
      this.convertFromJsonIndexMap = new HashMap<>();
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getOperator().getName().equalsIgnoreCase("convert_fromjson")) {
        List<RexNode> args = call.getOperands();
        Preconditions.checkArgument(args.size() == 1);
        final RexNode input = args.get(0);
        if (input instanceof RexInputRef) {
          // Copy the convert_fromjson expression to be added to the project below
          // and replace it with its reference
          RexInputRef rexInputRef = (RexInputRef) input;
          int index = rexInputRef.getIndex();
          if (!convertFromJsonIndexMap.containsKey(index)) {
            convertFromJsonList.add(call);
            // Put this index in the map as key and the value as the location in the list
            // so that we can retrieve it later if we see this again
            convertFromJsonIndexMap.put(index, convertFromJsonList.size() - 1); // -1 because of 0 valued index
          }
          return rexBuilder.makeInputRef(
            call.getType(),
            fieldCount + convertFromJsonIndexMap.get(index));
        } // else pass through
      }

      return super.visitCall(call);
    }
  }

  /**
   * Finds ITEM/DOT operators and replaces them with their arguments reference
   * in the project below.
   */
  private static final class FieldAccessFunctionReplacer extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final int fieldCount;
    private final List<RexNode> fieldAccessFunctionList;

    public FieldAccessFunctionReplacer(RexBuilder rexBuilder, int fieldCount,
                                       List<RexNode> fieldAccessFunctionList) {
      this.rexBuilder = rexBuilder;
      this.fieldCount = fieldCount; // Used for shift index because we push down at the end of the Project below
      this.fieldAccessFunctionList = fieldAccessFunctionList;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getOperator() == SqlStdOperatorTable.ITEM || call.getOperator() == SqlStdOperatorTable.DOT) {
        List<RexNode> args = call.getOperands();
        Preconditions.checkArgument(args.size() == 2);
        final RexNode rexNode = args.get(0);
        if (canPushdown(rexNode)) {
          fieldAccessFunctionList.add(rexNode);
          return call.clone(call.getType(),
            ImmutableList.of(rexBuilder.makeInputRef(
                rexNode.getType(),
                fieldCount + fieldAccessFunctionList.size() - 1), // -1 because of 0 valued index
              args.get(1))
          );
        }
      } // else pass through

      return super.visitCall(call);
    }

    private boolean canPushdown(RexNode rexNode) {
      return
        // If it's a simple reference to a field, we don't have to push it down.
        !(rexNode instanceof RexInputRef) &&
          // If the function argument is last_matching_map_entry_for_key, don't push it down. We rewrote it for reflection matching purposes.
          !(rexNode instanceof RexCall && ((RexCall) rexNode).getOperator() == LAST_MATCHING_MAP_ENTRY_FOR_KEY);
    }
  }

  /**
   * Pair of:
   * - Pushed down Project and
   * - List of top expressions which are updated after we push down convert_fromJson
   */
  static final class Pair {

    public static Pair NO_OP = new Pair(null, null);

    private final ProjectPrel projectPrel;
    private final List<RexNode> replacedExprs;

    public Pair(ProjectPrel projectPrel, List<RexNode> replacedExprs) {
      this.projectPrel = projectPrel;
      this.replacedExprs = replacedExprs;
    }
  }
}
