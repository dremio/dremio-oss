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
package com.dremio.exec.catalog.udf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableList;

public final class ParameterizedQueryParameterReplacer {
  public static RelNode replaceParameters(
    RelNode functionPlan,
    List<FunctionParameter> functionParameters,
    List<RexNode> values,
    RexBuilder rexBuilder) {
    return functionPlan.accept(
      createRelParameterReplacer(
        functionParameters,
        values,
        rexBuilder));
  }

  public static RelShuttle createRelParameterReplacer(
    List<FunctionParameter> functionParameters,
    List<RexNode> values,
    RexBuilder rexBuilder) {
    return new RelArgumentReplacer((RexArgumentReplacer)
      createRexParameterReplacer(
        functionParameters,
        values,
        rexBuilder));
  }

  public static RexShuttle createRexParameterReplacer(
    List<FunctionParameter> functionParameters,
    List<RexNode> values,
    RexBuilder rexBuilder) {
    Map<String, RexNode> replacementMapping = createReplacementMapping(functionParameters, values, rexBuilder);
    RexArgumentReplacer replacer = new RexArgumentReplacer(replacementMapping);
    return replacer;
  }

  public static RexNode replaceParameters(
    RexNode functionExpression,
    List<FunctionParameter> functionParameters,
    List<RexNode> values,
    RexBuilder rexBuilder) {
    return functionExpression.accept(
      createRexParameterReplacer(
        functionParameters,
        values,
        rexBuilder));
  }

  private static Map<String, RexNode> createReplacementMapping(
    List<FunctionParameter> functionParameters,
    List<RexNode> values,
    RexBuilder rexBuilder) {
    if (functionParameters.size() != values.size()) {
      throw new UnsupportedOperationException("Parameters and Replacements weren't the same size");
    }

    Map<String, RexNode> replacementMapping = new HashMap<>();
    for (int i = 0; i < functionParameters.size(); i++) {
      FunctionParameter functionParameter = functionParameters.get(i);
      RelDataType parameterType = functionParameter.getType(JavaTypeFactoryImpl.INSTANCE);
      String key = functionParameter.getName().toUpperCase();
      RexNode value = values.get(i);
      RelDataType valueType = value.getType();
      if ((valueType.getSqlTypeName() != SqlTypeName.ANY) && (valueType!= parameterType)) {
        // If value type is ANY, then don't even bother with the cast
        // It's probably a function like CONVERT_FROM that type validation bug that needs to be fixed.
        // Now if the types don't match and we know the correct type, then add a cast
        // If it's not castable, then we will get the appropriate error message.
        value = rexBuilder.makeCast(parameterType, value);
      }

      replacementMapping.put(key, value);
    }

    return replacementMapping;
  }

  // TODO: If calcite took the nameToNodeMap and didn't throw an exception if the name didn't exist,
  //  then we could call into convertSelect(...) with the mapping, but for now we will manually replace the UDF arguments.
  private static final class RelArgumentReplacer extends StatelessRelShuttleImpl {
    private final RexArgumentReplacer rexArgumentReplacer;

    public RelArgumentReplacer(RexArgumentReplacer rexArgumentReplacer) {
      this.rexArgumentReplacer = rexArgumentReplacer;
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode rewrittenInput = project.getInput().accept(this);
      LogicalProject rewrittenProject = (LogicalProject) project.accept(rexArgumentReplacer);
      boolean rewriteHappened = (rewrittenInput != project.getInput()) || (rewrittenProject != project);
      if (!rewriteHappened) {
        return project;
      }

      return rewrittenProject.copy(
        rewrittenProject.getTraitSet(),
        ImmutableList.of(rewrittenInput));
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode rewrittenInput = filter.getInput().accept(this);
      LogicalFilter rewrittenFilter = (LogicalFilter) filter.accept(rexArgumentReplacer);
      boolean rewriteHappened = (rewrittenInput != filter.getInput()) || (rewrittenFilter != filter);
      if (!rewriteHappened) {
        return filter;
      }

      return rewrittenFilter.copy(
        rewrittenFilter.getTraitSet(),
        rewrittenInput,
        rewrittenFilter.getCondition());
    }
  }

  private static final class RexArgumentReplacer extends RexShuttle {
    private final Map<String, RexNode> mapping;

    public RexArgumentReplacer(Map<String, RexNode> mapping) {
      this.mapping = mapping;
    }

    @Override public RexNode visitCall(RexCall call) {
      if (mapping.isEmpty()) {
        return call;
      }

      RexNode replacement = mapping.get(call.getOperator().getName().toUpperCase());
      if (replacement == null) {
        return super.visitCall(call);
      }

      return replacement;
    }
  }
}
