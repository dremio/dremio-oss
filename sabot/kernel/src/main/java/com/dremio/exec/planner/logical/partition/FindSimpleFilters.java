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
package com.dremio.exec.planner.logical.partition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * Adds support for pushing down simple filter conditions.
 * Includes: <, >, <=, >=, =, !=, against a literal.
 * We only handle ANDs (no ORs)
 */
public class FindSimpleFilters extends RexVisitorImpl<FindSimpleFilters.StateHolder> {

  public enum Type {CONDITION, INPUT, LITERAL, OTHER}

  private final RexBuilder builder;
  private final boolean sameTypesOnly;
  private final boolean fieldAccessSupport;

  public FindSimpleFilters(RexBuilder builder) {
    this(builder, false, true);
  }

  public FindSimpleFilters(RexBuilder builder, boolean sameTypesOnly) {
    this(builder, sameTypesOnly, true);
  }

  public FindSimpleFilters(RexBuilder builder, boolean sameTypesOnly, boolean fieldAccessSupport) {
    super(true);
    this.builder = builder;
    this.sameTypesOnly = sameTypesOnly;
    this.fieldAccessSupport = fieldAccessSupport;
  }


  public static class StateHolder {
    private final Type type;
    private final RexNode node;
    private final ImmutableList<RexCall> conditions;

    public StateHolder(Type type, RexNode node) {
      super();
      this.type = type;
      this.node = node;
      this.conditions = ImmutableList.of();
    }

    public StateHolder(Type type, RexNode original, ImmutableList<RexCall> conditions) {
      super();
      this.type = type;
      this.node = original;
      this.conditions = conditions;
    }

    public StateHolder add(RexCall carried){
      ImmutableList<RexCall> newConditions = ImmutableList.<RexCall>builder().addAll(conditions).add(carried).build();
      return new StateHolder(type, node, newConditions);
    }

    public StateHolder add(Collection<RexCall> calls){
      ImmutableList<RexCall> newConditions = ImmutableList.<RexCall>builder().addAll(conditions).addAll(calls).build();
      return new StateHolder(type, node, newConditions);
    }

    public ImmutableList<RexCall> getConditions() {
      return conditions;
    }

    public RexNode getNode(){
      Preconditions.checkNotNull(node);
      return node;
    }

    public boolean hasConditions(){
      return !conditions.isEmpty();
    }

    public boolean hasRemainingExpression(){
      return node != null;
    }

  }

  @Override
  public StateHolder visitInputRef(RexInputRef inputRef) {
    return new StateHolder(Type.INPUT, inputRef);
  }

  @Override
  public StateHolder visitLocalRef(RexLocalRef localRef) {
    return new StateHolder(Type.OTHER, localRef);
  }

  @Override
  public StateHolder visitLiteral(RexLiteral literal) {
    if (literal.isNull()) {
      return new StateHolder(Type.OTHER, literal);
    }
    return new StateHolder(Type.LITERAL, literal);
  }

  @Override
  public StateHolder visitOver(RexOver over) {
    return new StateHolder(Type.OTHER, over);
  }

  @Override
  public StateHolder visitCorrelVariable(RexCorrelVariable correlVariable) {
    return new StateHolder(Type.OTHER, correlVariable);
  }

  @Override
  public StateHolder visitCall(RexCall call) {
    switch(call.getKind()){
    case LESS_THAN:
    case GREATER_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
    case EQUALS:
    case IS_NOT_DISTINCT_FROM: // Add support to push (Input IS NOT DISTINCT FROM Constant)
    {
      List<RexNode> ops = call.getOperands();
      StateHolder a = ops.get(0).accept(this);
      StateHolder b = ops.get(1).accept(this);
      if(
          ((a.type == Type.LITERAL && b.type == Type.INPUT) ||
          (b.type == Type.LITERAL && a.type == Type.INPUT))
          && (!sameTypesOnly || MoreRelOptUtil.areDataTypesEqual(a.node.getType(), b.node.getType(), true))
          ){
        // this is a simple condition. Let's return a replacement
        if (call.getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
          /*
           * Filter conditions like "A IS NOT DISTINCT FROM B" can be rewritten as:
           * (NOT (A <> B OR A IS NULL OR B IS NULL) OR (A IS NULL AND B IS NULL))
           *
           * But since this visitor makes sure that if one side is INPUT, the other
           * side is a LITERAL, which is not a NULL but a constant, B can not be null.
           * Hence this can be simplified to:
           *
           * (NOT (A <> B OR A IS NULL))
           *
           * or:
           *
           * A = B AND A IS NOT NULL
           *
           * So, e.g. (IS NOT DISTINCT FROM($0, CAST(3):INTEGER)) can be simplified to
           * AND[=($0, 3), IS NOT NULL($0)]
           */

          RexNode input = a.type == Type.INPUT ? a.node : b.node;
          RexNode literal = a.type == Type.INPUT ? b.node : a.node;
          StateHolder holder = new StateHolder(Type.CONDITION, null)
            .add((RexCall) builder.makeCall(
              call.getType(),
              SqlStdOperatorTable.EQUALS,
              Arrays.asList(input, literal)));
          if (input.getType().isNullable()) {
            holder = holder.add((RexCall) builder.makeCall(
              call.getType(),
              SqlStdOperatorTable.IS_NOT_NULL,
              Collections.singletonList(input)));
          }
          return holder;
        }
        return new StateHolder(Type.CONDITION, null)
            .add((RexCall) builder.makeCall(call.getType(), call.getOperator(), Arrays.asList(a.node, b.node)));
      } else {
        // the two inputs are not literals/direct inputs.
        return new StateHolder(Type.OTHER, call);
      }
    }

    case AND:
    {
      List<RexNode> ops = call.getOperands();
      StateHolder a = ops.get(0).accept(this);
      for (int i = 1; i < ops.size(); i++) {
        StateHolder b = ops.get(i).accept(this);
        if(a.type == Type.CONDITION && b.type == Type.CONDITION) {
          a = new StateHolder(Type.CONDITION, composeConjunction(a.node, b.node)).add(a.conditions).add(b.conditions);
        } else if(a.type == Type.CONDITION) {
          a = new StateHolder(Type.CONDITION, composeConjunction(a.node, b.node)).add(a.conditions);
        } else if(b.type == Type.CONDITION) {
          a = new StateHolder(Type.CONDITION, composeConjunction(a.node, b.node)).add(b.conditions);
        } else {
          a = new StateHolder(a.type, composeConjunction(a.node, b.node));
        }
      }

      if (a.type == Type.CONDITION) {
        return a;
      }
    }

    case CAST:
    {
      if (SqlTypeName.ANY == call.getType().getSqlTypeName() ||
        (call.getOperands().size() == 1 && call.getOperands().get(0) instanceof RexLiteral)) { // If its a single literal cast
        return call.getOperands().get(0).accept(this);
      }

      // fallthrough
    }

    default:
      return new StateHolder(Type.OTHER, call);
    }

  }

  private RexNode composeConjunction(RexNode a, RexNode b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    } else {
      return RexUtil.composeConjunction(builder, Lists.newArrayList(a, b), false);
    }
  }

  @Override
  public StateHolder visitDynamicParam(RexDynamicParam dynamicParam) {
    return new StateHolder(Type.OTHER, dynamicParam);
  }

  @Override
  public StateHolder visitRangeRef(RexRangeRef rangeRef) {
    return new StateHolder(Type.OTHER, rangeRef);
  }

  @Override
  public StateHolder visitFieldAccess(RexFieldAccess fieldAccess) {
    if (fieldAccessSupport) {
      return new StateHolder(Type.INPUT, fieldAccess);
    }
    return new StateHolder(Type.OTHER, fieldAccess);
  }

  @Override
  public StateHolder visitSubQuery(RexSubQuery subQuery) {
    return new StateHolder(Type.OTHER, subQuery);
  }


}
