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
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.NlsString;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.sql.Checker;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.exec.util.ApproximateStringMatcher;
import com.google.common.collect.ImmutableList;

public final class RewriteConvertFunctionVisitor extends StatelessRelShuttleImpl {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RewriteConvertFunctionVisitor.class);
  private final RenameConvertToConvertFromVisitor renameConvertToConvertFromVisitor;

  public RewriteConvertFunctionVisitor(RexBuilder rexBuilder, OperatorTable table) {
    renameConvertToConvertFromVisitor = new RenameConvertToConvertFromVisitor(rexBuilder, table);
  }

  public static RelNode process(RelNode relNode, OperatorTable table){
    RewriteConvertFunctionVisitor convertRewriteVisitor = new RewriteConvertFunctionVisitor(relNode.getCluster().getRexBuilder(), table);
    return relNode.accept(convertRewriteVisitor);
  }

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    RexBuilder rexBuilder = logicalFilter.getCluster().getRexBuilder();

    RelNode rewrittenInput = logicalFilter.getInput().accept(this);
    RexNode rewrittenCondition = logicalFilter.getCondition().accept(renameConvertToConvertFromVisitor);
    boolean rewriteHappened = (rewrittenInput != logicalFilter.getInput()) || (rewrittenCondition != logicalFilter.getCondition());
    if (!rewriteHappened) {
      return logicalFilter;
    }

    return logicalFilter.copy(
      logicalFilter.getTraitSet(),
      rewrittenInput,
      rewrittenCondition);
  }

  @Override
  public RelNode visit(LogicalProject logicalProject) {
    RexBuilder rexBuilder = logicalProject.getCluster().getRexBuilder();

    RelNode rewrittenInput = logicalProject.getInput().accept(this);

    boolean rewriteHappened = rewrittenInput != logicalProject.getInput();
    List<RexNode> rewrittenProjects = new ArrayList<>();
    for (RexNode project : logicalProject.getProjects()) {
      RexNode rewrittenProject = project.accept(renameConvertToConvertFromVisitor);
      if (rewrittenProject != project) {
        rewriteHappened = true;
      }

      rewrittenProjects.add(rewrittenProject);
    }

    if (!rewriteHappened) {
      return logicalProject;
    }

    return logicalProject.copy(
      logicalProject.getTraitSet(),
      logicalProject.getInput(),
      rewrittenProjects,
      logicalProject.getRowType());
  }

  @Override
  public RelNode visit(LogicalJoin logicalJoin) {
    RexBuilder rexBuilder = logicalJoin.getCluster().getRexBuilder();

    RelNode rewrittenLeft = logicalJoin.getLeft().accept(this);
    RelNode rewrittenRight = logicalJoin.getRight().accept(this);
    RexNode rewrittenCondition = logicalJoin.getCondition().accept(renameConvertToConvertFromVisitor);

    boolean rewriteHappened = (rewrittenLeft != logicalJoin.getLeft())
      || (rewrittenRight != logicalJoin.getRight())
      || (rewrittenCondition != logicalJoin.getCondition());

    if (!rewriteHappened) {
      return logicalJoin;
    }

    return logicalJoin.copy(
      logicalJoin.getTraitSet(),
      rewrittenCondition,
      rewrittenLeft, rewrittenRight,
      logicalJoin.getJoinType(),
      logicalJoin.isSemiJoinDone());
  }

  public static class RenameConvertToConvertFromVisitor extends RexShuttle {

    private final RexBuilder builder;
    private final OperatorTable table;

    public RenameConvertToConvertFromVisitor(RexBuilder builder, OperatorTable table) {
      this.builder = builder;
      this.table = table;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      final String functionName = call.getOperator().getName();

      // check if it's a convert_from or convert_to function
      if (!"convert_from".equalsIgnoreCase(functionName) && !"convert_to".equalsIgnoreCase(functionName)) {
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
      } else if (nArgs > 3 || (nArgs > 2 && "convert_to".equalsIgnoreCase(functionName))) {
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
        if (!"utf8".equalsIgnoreCase(literal)) {
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

      // Find the SqlFunction with the correct args
      Optional<SqlOperator> newFunctionOperator = table
        .getSqlOperator(newFunctionName)
        .stream()
        .filter(op -> op.getOperandTypeChecker().getOperandCountRange().isValidCount(nArgs - 1))
        .findAny();

      if (!newFunctionOperator.isPresent()) {
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

    List<RexNode> operands = nArgs == 2 ? clonedOperands.subList(0, 1) : ImmutableList.of(clonedOperands.get(0), clonedOperands.get(2));

    return builder.makeCall(sqlOperator, operands);
    }

    private static UserException getConvertFunctionInvalidTypeException(final RexCall function) {
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
}
