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
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.exception.UnsupportedOperatorCollector;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.sql.Checker;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.exec.util.ApproximateStringMatcher;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This class rewrites all the project expression that contain convert_to/ convert_from
 * to actual implementations.
 * Eg: convert_from(EXPR, 'JSON') is rewritten as convert_fromjson(EXPR)
 *
 * With the actual method name we can find out if the function has a complex
 * output type and we will fire/ ignore certain rules (merge project rule) based on this fact.
 */
public class PreProcessRel extends StatelessRelShuttleImpl {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PreProcessRel.class);

  private final OperatorTable table;
  private final UnsupportedOperatorCollector unsupportedOperatorCollector;
  private final UnwrappingExpressionVisitor unwrappingExpressionVisitor;
  private final ConvertItemToInnerMapFunctionVisitor.RewriteItemOperatorVisitor rewriteItemOperatorVisitor;


  public static PreProcessRel createVisitor(OperatorTable table, RexBuilder rexBuilder) {
    return new PreProcessRel(table, rexBuilder);
  }

  private PreProcessRel(OperatorTable table, RexBuilder rexBuilder) {
    super();
    this.table = table;
    this.unsupportedOperatorCollector = new UnsupportedOperatorCollector();
    this.unwrappingExpressionVisitor = new UnwrappingExpressionVisitor(rexBuilder);
    this.rewriteItemOperatorVisitor = new ConvertItemToInnerMapFunctionVisitor.RewriteItemOperatorVisitor(rexBuilder);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    final RenameConvertToConvertFromVisitor renameVisitor = new RenameConvertToConvertFromVisitor(project.getCluster().getRexBuilder(), table);
    final List<RexNode> projExpr = Lists.newArrayList();
    for(RexNode rexNode : project.getProjects()) {
      projExpr.add(rexNode.accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor));
    }

    project =  project.copy(project.getTraitSet(),
        project.getInput(),
        projExpr,
        project.getRowType());

    List<RexNode> exprList = new ArrayList<>();
    boolean rewrite = false;

    for (RexNode rex : project.getProjects()) {
      RexNode newExpr = rex.accept(renameVisitor);
      if (newExpr != rex) {
        if (newExpr instanceof RexCall) {
          RexCall newExprCall = ((RexCall) newExpr);
          if (newExprCall.op.getName().equals("CONVERT_TOJSON") && newExprCall.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.ANY) {
            throw UserException.unsupportedError().message("Conversion to json is not supported for columns of mixed types.").buildSilently();
          }
        }
        rewrite = true;
      }
      exprList.add(newExpr);
    }

    if (rewrite) {
      LogicalProject newProject = project.copy(project.getTraitSet(), project.getInput(0), exprList, project.getRowType());
      return visitChild(newProject, 0, project.getInput());
    }

    return visitChild(project, 0, project.getInput());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    final RexNode condition = filter.getCondition().accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor);
    filter = filter.copy(
        filter.getTraitSet(),
        filter.getInput(),
        condition);
    return visitChild(filter, 0, filter.getInput());
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    final RexNode conditionExpr = join.getCondition().accept(unwrappingExpressionVisitor).accept(rewriteItemOperatorVisitor);
    join = join.copy(join.getTraitSet(),
        conditionExpr,
        join.getLeft(),
        join.getRight(),
        join.getJoinType(),
        join.isSemiJoinDone());

    return visitChildren(join);
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    for(RelNode child : union.getInputs()) {
      for(RelDataTypeField dataField : child.getRowType().getFieldList()) {
        if(dataField.getName().contains(StarColumnHelper.STAR_COLUMN)) {
          // see DRILL-2414
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
              "Union-All over schema-less tables must specify the columns explicitly");
          throw new UnsupportedOperationException();
        }
      }
    }

    return visitChildren(union);
  }

  public void convertException() throws SqlUnsupportedException {
    unsupportedOperatorCollector.convertException();
  }

  private static class RenameConvertToConvertFromVisitor extends RexShuttle {

    private final RexBuilder builder;
    private final OperatorTable table;

    public RenameConvertToConvertFromVisitor(RexBuilder builder, OperatorTable table) {
      this.builder = builder;
      this.table = table;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      final String functionName = call.getOperator().getName();

      // check if its a convert_from or convert_to function
      if (!functionName.equalsIgnoreCase("convert_from") && !functionName.equalsIgnoreCase("convert_to")) {
        return super.visitCall(call);
      }

      boolean[] update = {false};
      final List<RexNode> clonedOperands = visitList(call.getOperands(), update);

      final int nArgs = clonedOperands.size();

      if (nArgs < 2) {
        // Second operand is missing
        throw UserException.parseError()
          .message("'%s' expects a string literal as a second argument.", functionName)
          .build(logger);
      } else if (nArgs > 3 || (nArgs > 2 && functionName.equalsIgnoreCase("convert_to"))) {
        // Too many operands (>2 for 'convert_to', or >3 for 'convert_from')
        throw UserException.parseError()
          .message("Too many operands (%d) for '%s'", nArgs, functionName)
          .build(logger);
      }

      if (!(clonedOperands.get(1) instanceof RexLiteral)) {
        // caused by user entering a non-literal
        throw getConvertFunctionInvalidTypeException(call);
      }

      if (nArgs == 3 && !(clonedOperands.get(2) instanceof RexLiteral)) {
        // caused by user entering a non-literal
        throw getConvertFunctionInvalidTypeException(call);
      }

      String literal;
      try {
        literal = ((NlsString) (((RexLiteral) clonedOperands.get(1)).getValue())).getValue();
      } catch (final ClassCastException e) {
        // Caused by user entering a value with a non-string literal
        throw getConvertFunctionInvalidTypeException(call);
      }

      // construct the new function name based on the input argument
      String newFunctionName = functionName + literal;
      if (nArgs == 3) {
        if (!literal.equalsIgnoreCase("utf8")) {
          throw UserException.parseError()
            .message("3-argument convert_from only supported for utf8 encoding. Instead, got %s", literal)
            .build(logger);
        }
        newFunctionName = "convert_replaceUTF8";
      }

      // Look up the new function name in the operator table
      List<SqlOperator> operatorList = table.getSqlOperator(newFunctionName);
      if (operatorList.size() == 0) {
        // User typed in an invalid type name
        throw getConvertFunctionException(functionName, literal);
      }
      SqlFunction newFunction = null;

      // Find the SqlFunction with the correct args
      for (SqlOperator op : operatorList) {
        if (op.getOperandTypeChecker().getOperandCountRange().isValidCount(nArgs - 1)) {
          newFunction = (SqlFunction) op;
          break;
        }
      }
      if (newFunction == null) {
        // we are here because we found some dummy convert function. (See DummyConvertFrom and DummyConvertTo)
        throw getConvertFunctionException(functionName, literal);
      }

      SqlFunction sqlOperator = SqlFunctionImpl.create(
        newFunctionName,
        new SqlReturnTypeInference() {
          @Override
          public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return call.getType();
          }
        },
        Checker.between(1, nArgs - 1));

      // create the new expression to be used in the rewritten project
      if (nArgs == 2) {
        return builder.makeCall(sqlOperator, clonedOperands.subList(0, 1));
      } else {
        // 3 arguments. The two arguments passed to the function are the first and last argument (the middle is the type)
        return builder.makeCall(sqlOperator, ImmutableList.of(clonedOperands.get(0), clonedOperands.get(2)));
      }
    }

    private UserException getConvertFunctionInvalidTypeException(final RexCall function) {
      // Caused by user entering a value with a numeric type
      final String functionName = function.getOperator().getName();
      final String typeName = function.getOperands().get(1).getType().getFullTypeString();
      return UserException.parseError()
        .message("Invalid type %s passed as second argument to function '%s'. " +
            "The function expects a literal argument.",
          typeName,
          functionName)
        .build(logger);
    }

    private UserException getConvertFunctionException(final String functionName, final String typeName) {
      final String newFunctionName = functionName + typeName;
      final boolean emptyTypeName = typeName.isEmpty();
      final String typeNameToPrint = emptyTypeName ? "<empty_string>" : typeName;
      final UserException.Builder exceptionBuilder = UserException.unsupportedError()
        .message("%s does not support conversion %s type '%s'.", functionName, functionName.substring(8).toLowerCase(), typeNameToPrint);
      // Build a nice error message
      if (!emptyTypeName) {
        List<String> ops = new ArrayList<>();
        for (SqlOperator op : table.getOperatorList()) {
          ops.add(op.getName());
        }
        final String bestMatch = ApproximateStringMatcher.getBestMatch(ops, newFunctionName);
        if (bestMatch != null && bestMatch.length() > 0 && bestMatch.toLowerCase().startsWith("convert")) {
          final StringBuilder s = new StringBuilder("Did you mean ")
            .append(bestMatch.substring(functionName.length()))
            .append("?");
          exceptionBuilder.addContext(s.toString());
        }
      }
      return exceptionBuilder.build(logger);
    }
  }

  private static class UnwrappingExpressionVisitor extends RexShuttle {
    private final RexBuilder rexBuilder;

    private UnwrappingExpressionVisitor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      final List<RexNode> clonedOperands = visitList(call.operands, new boolean[]{true});
      final SqlOperator sqlOperator = call.getOperator();
      return RexUtil.flatten(rexBuilder,
          rexBuilder.makeCall(
              call.getType(),
              sqlOperator,
              clonedOperands));
    }
  }

}
