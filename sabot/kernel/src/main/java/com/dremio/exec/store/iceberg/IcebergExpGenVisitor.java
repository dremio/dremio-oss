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
package com.dremio.exec.store.iceberg;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.Lists;

public class IcebergExpGenVisitor extends RexVisitorImpl<Expression> {

    private final List<String> fieldNames;
    private final RexBuilder rexBuilder;
    private final RelOptCluster relOptCluster;
    private Set<String> usedColumns;

    public IcebergExpGenVisitor(RelDataType rowType, RelOptCluster cluster) {
        super(true);
        this.fieldNames = rowType.getFieldNames();
        this.rexBuilder = cluster.getRexBuilder();
        this.relOptCluster = cluster;
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        return Expressions.alwaysTrue();
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        return Expressions.alwaysTrue();
    }

    @Override
    public Expression visitCall(RexCall call) {
        final RexNode arg1 = call.getOperands().get(0);
        final RexNode arg2 = call.getOperands().get(1);
        RexInputRef inputRef = null;
        RexNode other = null;
        boolean inputFirst = false;
        if ((arg1 instanceof RexInputRef) && (arg2 instanceof RexLiteral)) {
            inputRef = (RexInputRef) arg1;
            other = arg2;
            inputFirst = true;
        } else if ((arg2 instanceof RexInputRef) && (arg1 instanceof RexLiteral)) {
            inputRef = (RexInputRef) arg2;
            other = arg1;
        }
        if (inputRef != null && other != null) {
            Object val = getValueAsInputRef(inputRef, (RexLiteral) other);
            String columnName = fieldNames.get(inputRef.getIndex());
            usedColumns.add(columnName);
            if (Objects.isNull(val)) {
                return Expressions.alwaysFalse();
            }
            return getOperatorExpression(call, columnName, val, inputFirst);
        } else {
            boolean isAND = false;
            switch (call.getOperator().getKind()) {
                case AND:
                    isAND = true;
                    // fall through
                case OR:
                    List<RexNode> nodeList = call.getOperands();
                    Expression left = nodeList.get(0).accept(this);
                    Expression right = nodeList.get(1).accept(this);
                    Expression[] expressions = (nodeList.size() > 2) ?
                            nodeList.subList(2, nodeList.size())
                                    .stream()
                                    .map(e -> e.accept(this))
                                    .collect(Collectors.toList()).toArray(new Expression[0]) : null;
                    if (isAND) {
                        return (expressions == null) ? Expressions.and(left, right) : Expressions.and(left, right, expressions);
                    } else {
                        Expression orExpression = Expressions.or(left, right);
                        if (expressions != null) {
                          orExpression = buildOrTree(left, right, expressions);
                        }
                        return orExpression;
                    }
                default:
                    throw UserException.validationError().message("Not a valid expression to convert into iceberg expression")
                            .buildSilently();
            }
        }
    }

  private Expression buildOrTree(Expression left, Expression right, Expression[] expressions) {
    List<Expression> workList = new ArrayList<>(expressions.length + 2);
    workList.add(left);
    workList.add(right);
    Collections.addAll(workList, expressions);
    int expressionsCount = workList.size();
    int i, j;
    while (expressionsCount > 1) {
      for (i = 0, j = 0; i < expressionsCount; ) {
        if (i + 1 < expressionsCount) {
          workList.set(j, Expressions.or(workList.get(i), workList.get(i + 1)));
        } else {
          workList.set(j, workList.get(i));
        }
        i += 2;
        j++;
      }
      expressionsCount = j;
    }
    return workList.get(0);
  }

  private Object getValueAsInputRef(RexInputRef inputRef, RexLiteral literal) {
        SqlTypeName sqlTypeName = inputRef.getType().getSqlTypeName();
        try {
            switch (sqlTypeName) {
                case BOOLEAN:
                    return convert(Boolean.class, (RexLiteral lit) -> Boolean.valueOf(lit.getValueAs(String.class)), literal);
                case INTEGER:
                    return convert(Integer.class, (RexLiteral lit) -> Integer.valueOf(lit.getValueAs(String.class)), literal);
                case BIGINT:
                    return convert(Long.class, (RexLiteral lit) -> Long.valueOf(lit.getValueAs(String.class)), literal);
                case FLOAT:
                    return convert(Float.class, (RexLiteral lit) -> Float.valueOf(lit.getValueAs(String.class)), literal);
                case DOUBLE:
                    return convert(Double.class, (RexLiteral lit) -> Double.valueOf(lit.getValueAs(String.class)), literal);
                case VARCHAR:
                    return literal.getValueAs(String.class);
                case DECIMAL:
                    return convert(BigDecimal.class, (RexLiteral lit) -> new BigDecimal(lit.getValueAs(String.class)), literal);
                case DATE:
                    if (literal.getTypeName() == SqlTypeName.CHAR) {
                        RexLiteral dateTypeLiteral = getLiteralOfType(SqlTypeName.DATE, literal);
                        return dateTypeLiteral.getValueAs(Integer.class);
                    }
                    return literal.getValueAs(Integer.class);
                case TIME:
                    if (literal.getTypeName() == SqlTypeName.CHAR) {
                        RexLiteral timeTypeLiteral = getLiteralOfType(SqlTypeName.TIME, literal);
                        return Long.valueOf(timeTypeLiteral.getValueAs(Integer.class)) * 1000;
                    }
                    return Long.valueOf(literal.getValueAs(Integer.class)) * 1000;
                case TIMESTAMP:
                    if (literal.getTypeName() == SqlTypeName.CHAR) {
                        RexLiteral timestampTypeLiteral = getLiteralOfType(SqlTypeName.TIMESTAMP, literal);
                        return timestampTypeLiteral.getValueAs(Long.class) * 1000;
                    }
                    return literal.getValueAs(Long.class) * 1000;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + sqlTypeName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert " + literal.toString() + " to type " + sqlTypeName, e);
        }
    }

    private Object convert(Class c, Function<RexLiteral, Comparable> f, RexLiteral literal) {
        if (literal.getTypeName() == SqlTypeName.CHAR) {
            return f.apply(literal);
        } else {
            return literal.getValueAs(c);
        }
    }

    private RexLiteral getLiteralOfType(SqlTypeName type, RexLiteral literal) {
        RexNode node = rexBuilder.makeCast(SqlTypeFactoryImpl.INSTANCE.createSqlType(type), literal);
        final List<RexNode> reducedValues = Lists.newArrayList();
        final List<RexNode> constExpNode = Lists.newArrayList();
        constExpNode.add(node);
        relOptCluster.getPlanner().getExecutor().reduce(rexBuilder, constExpNode, reducedValues);
        return (RexLiteral) reducedValues.get(0);
    }

    public Expression getNullCheckExpression(Expression expression) {
        Expression nullCheckExpression = null;
        for (String columnName: usedColumns) {
            nullCheckExpression = (nullCheckExpression != null)?
                    Expressions.and(Expressions.notNull(columnName), nullCheckExpression):
                    Expressions.notNull(columnName);
        }
        return Expressions.and(nullCheckExpression, expression);
    }

    private Expression getOperatorExpression(RexCall call, String columnName, Object val, Boolean inputFirst) {
        switch (call.getOperator().getKind()) {
            case LESS_THAN:
                return inputFirst ?
                        Expressions.lessThan(columnName, val) :
                        Expressions.greaterThan(columnName, val);
            case LESS_THAN_OR_EQUAL:
                return inputFirst ?
                        Expressions.lessThanOrEqual(columnName, val) :
                        Expressions.greaterThanOrEqual(columnName, val);
            case GREATER_THAN:
                return inputFirst ?
                        Expressions.greaterThan(columnName, val) :
                        Expressions.lessThan(columnName, val);
            case GREATER_THAN_OR_EQUAL:
                return inputFirst ?
                        Expressions.greaterThanOrEqual(columnName, val) :
                        Expressions.lessThanOrEqual(columnName, val);
            case EQUALS:
                return Expressions.equal(columnName, val);
            case NOT_EQUALS:
                return Expressions.notEqual(columnName, val);
            default:
                throw UserException.validationError().message("Not a valid expression to convert into iceberg expression")
                        .buildSilently();
        }
    }

    public Expression convertToIcebergExpression(RexNode condition) {
        usedColumns = new HashSet<>();
        Expression icebergExpression = condition.accept(this);
        return getNullCheckExpression(icebergExpression);
    }
}
