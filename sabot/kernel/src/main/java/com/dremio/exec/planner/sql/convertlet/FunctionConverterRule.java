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
package com.dremio.exec.planner.sql.convertlet;

import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.RelDataTypeEqualityUtil;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.RexShuttleRelShuttle;
import com.dremio.options.OptionResolver;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

public final class FunctionConverterRule extends RelRule<RelRule.Config> {
  private final List<RexNodeConvertlet> convertlets;

  public FunctionConverterRule(
      OptionResolver optionResolver, UserDefinedFunctionExpander udfExpander) {
    super(
        Config.EMPTY
            .withDescription("FunctionConvertionRule")
            // Don't operate on the leaf relnode, since relNode.getInput(0) will throw an exception
            .withOperandSupplier(
                op ->
                    op.operand(RelNode.class)
                        .oneInput(input -> input.operand(RelNode.class).anyInputs())));
    convertlets = new ArrayList<>();
    convertlets.add(ArrayAppendConvertlet.INSTANCE);
    convertlets.add(ArrayAvgConvertlet.INSTANCE);
    convertlets.add(ArrayCastConvertlet.INSTANCE);
    convertlets.add(ArrayConcatConvertlet.INSTANCE);
    convertlets.add(ArrayContainsConvertlet.INSTANCE);
    convertlets.add(ArrayDistinctConvertlet.INSTANCE);
    convertlets.add(ArrayIntersectionConvertlet.INSTANCE);
    convertlets.add(ArrayPrependConvertlet.INSTANCE);
    convertlets.add(ArraysOverlapConvertlet.INSTANCE);
    convertlets.add(ArrayValueConstructorConvertlet.INSTANCE);
    convertlets.add(ConvertFromConvertlet.INSTANCE);
    convertlets.add(ConvertToConvertlet.INSTANCE);
    convertlets.add(IndexingOnMapConvertlet.INSTANCE);
    convertlets.add(LikeToColumnLikeConvertlet.LIKE_TO_COL_LIKE);
    convertlets.add(LikeToColumnLikeConvertlet.REGEXP_LIKE_TO_REGEXP_COL_LIKE);
    convertlets.add(MapConstructConvertlet.INSTANCE);
    convertlets.add(MapValueConstructorConvertlet.INSTANCE);
    convertlets.add(RegexpLikeToLikeConvertlet.INSTANCE);
    convertlets.add(SetUnionConvertlet.INSTANCE);
    convertlets.add(new UdfConvertlet(udfExpander));
    if (optionResolver.getOption(PlannerSettings.REDUCE_ALGEBRAIC_EXPRESSIONS)) {
      convertlets.add(SimpleTrigArithmeticConvertlet.INSTANCE);
      convertlets.add(InverseTrigConvertlet.INSTANCE);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode = call.rel(0);
    RelNode rewrittenRelNode = convert(relNode, convertlets);
    if (relNode != rewrittenRelNode) {
      call.transformTo(rewrittenRelNode);
    }
  }

  public static RelNode convert(RelNode relNode, List<RexNodeConvertlet> convertlets) {
    RelBuilder relBuilder =
        DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(relNode.getCluster(), null);
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();

    ConvertletContext convertletContext =
        new ConvertletContext(
            () ->
                (RexCorrelVariable)
                    rexBuilder.makeCorrel(
                        relNode.getInput(0).getRowType(), relNode.getCluster().createCorrel()),
            relBuilder,
            rexBuilder);
    RexShuttleRelShuttle shuttle =
        new RexShuttleRelShuttle(new RexShuttleImpl(convertletContext, convertlets));

    return relNode.accept(shuttle);
  }

  private static final class RexShuttleImpl extends RexShuttle {
    private final ConvertletContext convertletContext;
    private final List<RexNodeConvertlet> convertlets;

    public RexShuttleImpl(
        ConvertletContext convertletContext, List<RexNodeConvertlet> convertlets) {
      this.convertletContext = convertletContext;
      this.convertlets = convertlets;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      // Recurse to visit the operands
      final RexNode operandsVisited = super.visitCall(call);

      RexNode previous = operandsVisited;
      while (true) {
        RexNode current = previous;
        // Keep applying rewrites as long as a transformation happens.
        for (RexNodeConvertlet functionConvertlet : convertlets) {
          if (functionConvertlet.matches(current)) {
            current = functionConvertlet.convert(convertletContext, current);
            assertTypesMatch(current, call);
          }
        }

        if (current == previous) {
          // No rewrite happened, so we can return the call
          return current;
        }

        // Do a recursive call on the whole thing,
        // since one of the rules might have rewritten one of the operands
        // and now the operand matches a rewrite rule
        previous = current.accept(this);
      }
    }

    private void assertTypesMatch(RexNode original, RexNode converted) {
      if (!RelDataTypeEqualityUtil.areEquals(
          original.getType(), converted.getType(), false, false)) {
        throw new RuntimeException(
            "RexNode conversion resulted in type mismatch.\n"
                + "Original Type: "
                + original.getType()
                + " nullable: "
                + original.getType().isNullable()
                + "\n"
                + "Converted Type: "
                + converted.getType()
                + " nullable: "
                + converted.getType().isNullable());
      }
    }
  }
}
