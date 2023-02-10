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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;

/**
 * Push down all the convert_fromjson in the project below to avoid
 * creating multiple ConvertFromJsonPrel nodes.
 */
public final class ConvertFromJsonPushDownVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final ConvertFromJsonPushDownVisitor INSTANCE = new ConvertFromJsonPushDownVisitor();

  public static Prel convertFromJsonPushDown(Prel prel, OptionManager options) {
    return options.getOption(PlannerSettings.CONVERT_FROM_JSON_PUSHDOWN) ?
      prel.accept(INSTANCE, null) :
      prel;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();

    for (Prel child : prel) {
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitProject(ProjectPrel prel, Void voidValue) throws RuntimeException {
    final List<RexNode> expressions = prel.getProjects();
    final RelNode inputRel = ((Prel) prel.getInput()).accept(this, null);
    final RelDataType inputRowType = inputRel.getRowType();
    final List<RexNode> convertFromJsonList = new ArrayList<>();
    final Map<Integer, Integer> convertFromJsonIndexMap = new HashMap<>();
    final List<RexNode> topExprs = new ArrayList<>();
    final ConvertFromJsonFinder finder = new ConvertFromJsonFinder(prel.getCluster().getRexBuilder(),
      inputRowType.getFieldCount(), convertFromJsonList, convertFromJsonIndexMap);
    expressions.forEach(rexNode -> topExprs.add(rexNode.accept(finder)));

    if (convertFromJsonList.isEmpty()) {
      // No convert_fromjson found. Return.
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((inputRel)));
    }

    // Push all the convert_fromjson's nodes in the project below.
    final RelOptCluster cluster = prel.getCluster();
    final RelDataTypeFactory factory = cluster.getTypeFactory();

    final List<RexNode> bottomExprs = new ArrayList<>();
    final List<RelDataType> bottomProjectType = new ArrayList<>();
    final List<String> fieldNameList = new ArrayList<>();

    for (int i = 0; i < inputRowType.getFieldCount(); i++) {
      final RelDataTypeField field = inputRowType.getFieldList().get(i);
      final RelDataType type = field.getType();
      fieldNameList.add(field.getName());
      bottomProjectType.add(type);
      bottomExprs.add(prel.getCluster().getRexBuilder().makeInputRef(type, i));
    }

    for (int i = 0; i < convertFromJsonList.size(); i++) {
      RexNode rexNode = convertFromJsonList.get(i);
      bottomExprs.add(rexNode);
      fieldNameList.add("CONVERT_FROM_JSON_" + i);
      bottomProjectType.add(rexNode.getType());
    }

    final RelDataType bottomType = factory.createStructType(bottomProjectType, fieldNameList);
    // Bottom project with convert_fromjson's appended at the end
    final ProjectPrel bottomProject = ProjectPrel.create(cluster, prel.getTraitSet(), inputRel, bottomExprs, bottomType);

    final List<RelDataType> topProjectType = topExprs.stream().map(RexNode::getType).collect(Collectors.toList());
    final RelDataType topType = factory.createStructType(topProjectType, prel.getRowType().getFieldNames());
    // Top project which references to convert_fromjson fields in the project below.
    return ProjectPrel.create(cluster, prel.getTraitSet(), bottomProject, topExprs, topType);
  }

  /**
   * Finds convert_fromjson nodes and replaces them with their reference
   * in the project below.
   */
  private static final class ConvertFromJsonFinder extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final int fields;
    private final List<RexNode> convertFromJsonList;
    private final Map<Integer, Integer> convertFromJsonIndexMap;

    public ConvertFromJsonFinder(RexBuilder rexBuilder, int fields,
                                 List<RexNode> convertFromJsonList,
                                 Map<Integer, Integer> convertFromJsonIndexMap) {
      this.rexBuilder = rexBuilder;
      this.fields = fields;
      this.convertFromJsonList = convertFromJsonList;
      this.convertFromJsonIndexMap = convertFromJsonIndexMap;
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
            fields + convertFromJsonIndexMap.get(index));
        } // else pass through
      }

      return super.visitCall(call);
    }
  }
}
