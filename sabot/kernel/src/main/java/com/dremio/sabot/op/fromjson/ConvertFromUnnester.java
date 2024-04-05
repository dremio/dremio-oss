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

import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * A lot of our rewrites for CONVERT_FROM assume that the function call exists at the root of the
 * rexnode. For example: CONVERT_FROM(x) is fine, but FOO(CONVERT_FROM(x)) is not fine. This code
 * rewrites:
 *
 * <p>FOO(CONVERT_FROM(x)) to FOO($0) where $0 is CONVERT_FROM(x)
 */
public final class ConvertFromUnnester extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private static final ConvertFromUnnester INSTANCE = new ConvertFromUnnester();

  private ConvertFromUnnester() {}

  public static Prel unnest(Prel node) {
    return INSTANCE.visitPrel(node, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for (Prel p : prel) {
      children.add(p.accept(this, null));
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitProject(ProjectPrel topRel, Void voidValue) throws RuntimeException {
    Prel rewrittenInput = ((Prel) topRel.getInput()).accept(this, null);

    List<RexNode> projects = topRel.getProjects();
    List<RexNode> replacements = new ArrayList<>();
    List<RexNode> convertFroms = new ArrayList<>();
    for (RexNode project : projects) {
      ConvertFromJsonReplacer replacer =
          new ConvertFromJsonReplacer(
              topRel.getCluster().getRexBuilder(),
              rewrittenInput.getRowType().getFieldCount() + convertFroms.size());
      RexNode replacment = project.accept(replacer);
      replacements.add(replacment);
      convertFroms.addAll(replacer.convertFroms);
    }

    if (convertFroms.isEmpty()) {
      return topRel.copy(
          topRel.getTraitSet(), rewrittenInput, topRel.getProjects(), topRel.getRowType());
    }

    RelDataTypeFactory.FieldInfoBuilder builder =
        topRel
            .getCluster()
            .getTypeFactory()
            .builder()
            .addAll(rewrittenInput.getRowType().getFieldList());
    for (int i = 0; i < convertFroms.size(); i++) {
      builder.add("replacement" + i, convertFroms.get(i).getType());
    }

    RelDataType newRelDataType = builder.build();
    List<RexNode> newProjects = new ArrayList<>();
    for (int i = 0; i < rewrittenInput.getRowType().getFieldCount(); i++) {
      RexNode passthrough = topRel.getCluster().getRexBuilder().makeInputRef(rewrittenInput, i);
      newProjects.add(passthrough);
    }

    newProjects.addAll(convertFroms);

    ProjectPrel newChild =
        ProjectPrel.create(
            topRel.getCluster(),
            rewrittenInput.getTraitSet(),
            rewrittenInput,
            newProjects,
            newRelDataType);
    ProjectPrel newTopRel =
        ProjectPrel.create(
            topRel.getCluster(), topRel.getTraitSet(), newChild, replacements, topRel.getRowType());
    return newTopRel;
  }

  private static final class ConvertFromJsonReplacer extends RexShuttle {
    private final List<RexNode> convertFroms;
    private final RexBuilder rexBuilder;
    private int counter;

    private ConvertFromJsonReplacer(RexBuilder rexBuilder, int counter) {
      this.convertFroms = new ArrayList<>();
      this.rexBuilder = rexBuilder;
      this.counter = counter;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      boolean noConvertFrom =
          call.getOperands().stream()
              .noneMatch(
                  operand ->
                      operand instanceof RexCall
                          && ((RexCall) operand).op.getName().equalsIgnoreCase("convert_fromjson"));
      if (noConvertFrom) {
        return super.visitCall(call);
      }

      List<RexNode> rewrittenOperands = new ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        RexNode rewrittenOperand;
        boolean isConvertFrom =
            operand instanceof RexCall
                && ((RexCall) operand).op.getName().equalsIgnoreCase("convert_fromjson");
        if (!isConvertFrom) {
          rewrittenOperand = operand.accept(this);
        } else {
          rewrittenOperand = rexBuilder.makeInputRef(operand.getType(), counter++);
          convertFroms.add(operand);
        }

        rewrittenOperands.add(rewrittenOperand);
      }

      // Keep the same return type to avoid an inferReturnType call that will fail for CAST
      return rexBuilder.makeCall(call.getType(), call.op, rewrittenOperands);
    }
  }
}
