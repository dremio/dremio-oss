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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class ScalarUserDefinedFunctionExpanderRule
  extends RelRule<RelRule.Config> {

  private final Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier;

  public ScalarUserDefinedFunctionExpanderRule(
    Config config,
    Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier) {
    super(config);
    this.sqlSubQueryConverterBuilderSupplier = sqlSubQueryConverterBuilderSupplier;
  }


  protected Pair<RelNode, CorrelationId> convert(RelNode rel) {
    CorrelationId correlationId = rel.getVariablesSet().isEmpty()
      ? rel.getCluster().createCorrel()
      : Iterables.getOnlyElement(rel.getVariablesSet());

    ScalarParserValidator scalarParserValidator = new ScalarParserValidator(sqlSubQueryConverterBuilderSupplier.get());
    UdfExpander udfExpander = new UdfExpander(scalarParserValidator, rel.getCluster().getRexBuilder());
    return Pair.of(rel.accept(udfExpander), correlationId);
  }

  public static RelOptRule createFilterRule(
    Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier){
    return new ScalarUserDefinedFunctionExpanderRule(
      Config.EMPTY
        .withDescription("ScalarUserDefinedFunctionExpanderRuleFilter")
        .withOperandSupplier(op1 ->
          op1.operand(Filter.class).anyInputs()),
      sqlSubQueryConverterBuilderSupplier) {

      @Override public void onMatch(RelOptRuleCall relOptRuleCall) {
        Filter rel = relOptRuleCall.rel(0);
        Pair<RelNode, CorrelationId> relAndCor = convert(rel);
        if (relAndCor.left == rel) {
          return;
        }

        relOptRuleCall.transformTo(relOptRuleCall.builder()
          .push(rel.getInput())
          .filter(ImmutableList.of(relAndCor.right), ImmutableList.of(((Filter) relAndCor.left).getCondition()))
          .build());
      }
    };
  }


  public static RelOptRule createProjectRule(
    Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier){
    return new ScalarUserDefinedFunctionExpanderRule(
      Config.EMPTY
        .withDescription("ScalarUserDefinedFunctionExpanderRuleProject")
        .withOperandSupplier(op1 ->
          op1.operand(Project.class).anyInputs()),
      sqlSubQueryConverterBuilderSupplier) {

      @Override public void onMatch(RelOptRuleCall relOptRuleCall) {
        Project rel = relOptRuleCall.rel(0);
        Pair<RelNode, CorrelationId> relAndCor = convert(rel);
        if (relAndCor.left == rel) {
          return;
        }

        //TODO project drop correlate node.....
        relOptRuleCall.transformTo(relAndCor.left);
      }
    };
  }
}

class ReplaceArgumentsVisitor extends RexShuttle {
  private final List<RexNode> arguments;
  private final List<SqlOperator> argumentsOperators;

  public ReplaceArgumentsVisitor(List<RexNode> arguments,
    List<SqlOperator> argumentsOperators) {
    this.arguments = arguments;
    this.argumentsOperators = argumentsOperators;
  }

  @Override public RexNode visitCall(RexCall call) {
    int index = argumentsOperators.indexOf(call.getOperator());
    if(index == -1) {
      return super.visitCall(call);
    } else {
      return arguments.get(index);
    }
  }

  @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
    RexShuttle rexShuttle = this;
    RelNode relNode = subQuery.rel.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        return other.accept(rexShuttle);
      }
    });
    List<RexNode> rexNodes = subQuery.getOperands().stream()
      .map(o -> o.accept(rexShuttle))
      .collect(ImmutableList.toImmutableList());

    return subQuery
      .clone(subQuery.type, rexNodes)
      .clone(relNode);
  }
}

class ScalarParserValidator {
  private final SqlValidatorAndToRelContext.Builder sqlSubQueryConverterBuilder;

  public ScalarParserValidator(SqlValidatorAndToRelContext.Builder sqlSubQueryConverterBuilder) {
    this.sqlSubQueryConverterBuilder = sqlSubQueryConverterBuilder;
  }

  public SqlValidatorAndToRelContext.FunctionBodyAndArguments expand(
    DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction) {
    SqlValidatorAndToRelContext sqlValidatorAndToRelContext = sqlSubQueryConverterBuilder
      .withSchemaPath(ImmutableList.of())
      .withUser(dremioScalarUserDefinedFunction.getOwner())
      .withContextualSqlOperatorTable(new FunctionOperatorTable(
        dremioScalarUserDefinedFunction.getName(),
        dremioScalarUserDefinedFunction.getParameters()))
      .build();


    SqlNode sqlNode = parse(sqlValidatorAndToRelContext.getSqlConverter(), dremioScalarUserDefinedFunction);
    return sqlValidatorAndToRelContext.validateAndConvertScalarFunction(sqlNode, dremioScalarUserDefinedFunction.getName(), dremioScalarUserDefinedFunction.getParameters());
  }

  private SqlNode parse(SqlConverter sqlConverter, DremioScalarUserDefinedFunction udf) {
    SqlNode sqlNode = sqlConverter.parse(udf.getFunctionSql());
    if (sqlNode instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlNode;
      Preconditions.checkState(null == sqlSelect.getFrom());
      Preconditions.checkState(sqlSelect.getSelectList().size() == 1);
      return sqlSelect.getSelectList().get(0);
    } else {
      throw new RuntimeException();
    }
  }
}



class UdfExpander extends RexShuttle {
  private final ScalarParserValidator scalarParserValidator;
  private final RexBuilder rexBuilder;

  public UdfExpander(ScalarParserValidator scalarParserValidator, RexBuilder rexBuilder) {
    this.scalarParserValidator = scalarParserValidator;
    this.rexBuilder = rexBuilder;
  }

  @Override public RexNode visitCall(RexCall call) {
    SqlOperator operator = call.getOperator();
    RexCall converted = (RexCall) super.visitCall(call);

    if (operator instanceof SqlUserDefinedFunction) {
      Function function = ((SqlUserDefinedFunction) operator).getFunction();
      if(function instanceof DremioScalarUserDefinedFunction) {

        DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction =
          (DremioScalarUserDefinedFunction) function;

        SqlValidatorAndToRelContext.FunctionBodyAndArguments functionBodyAndArguments =
          scalarParserValidator.expand(dremioScalarUserDefinedFunction);
        List<RexNode> paramRexList = converted.getOperands();
        List<RexNode> transformedArguments = new ArrayList<>();
        Preconditions.checkState(function.getParameters().size() == paramRexList.size());
        for (int i = 0; i < paramRexList.size(); i++) {
          RexNode paramRex = paramRexList.get(i);
          FunctionParameter param = dremioScalarUserDefinedFunction.getParameters().get(i);
          transformedArguments.add(rexBuilder.makeCast(param.getType(rexBuilder.getTypeFactory()), paramRex));
        }

        ReplaceArgumentsVisitor replaceArgumentsVisitor = new ReplaceArgumentsVisitor(
          transformedArguments,
          functionBodyAndArguments.getUserDefinedFunctionArgumentOperators());
        RexNode expandedNode = functionBodyAndArguments.getFunctionBody()
          .accept(replaceArgumentsVisitor)
          .accept(this);
        return rexBuilder.makeCast(call.getType(), expandedNode, true);
      }
    }
    return converted;
  }
}
