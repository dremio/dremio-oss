/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package org.apache.calcite.rex;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Passes over a row-expression, calling a handler method for each node,
 * appropriate to the type of the node.
 *
 * <p>Like {@link RexVisitor}, this is an instance of the
 * {@link org.apache.calcite.util.Glossary#VISITOR_PATTERN Visitor Pattern}. Use
 * <code> RexShuttle</code> if you would like your methods to return a
 * value.</p>
 */
public class RexBiShuttle<P> implements RexBiVisitor<RexNode, P> {
  //~ Methods ----------------------------------------------------------------

  public RexNode visitOver(RexOver over, P arg) {
    boolean[] update = {false};
    List<RexNode> clonedOperands = visitList(over.operands, update, arg);
    RexWindow window = visitWindow(over.getWindow(), arg);
    if (update[0] || (window != over.getWindow())) {
      // REVIEW jvs 8-Mar-2005:  This doesn't take into account
      // the fact that a rewrite may have changed the result type.
      // To do that, we would need to take a RexBuilder and
      // watch out for special operators like CAST and NEW where
      // the type is embedded in the original call.
      return new RexOver(
          over.getType(),
          over.getAggOperator(),
          clonedOperands,
          window);
    } else {
      return over;
    }
  }

  public RexWindow visitWindow(RexWindow window, P arg) {
    boolean[] update = {false};
    List<RexFieldCollation> clonedOrderKeys =
        visitFieldCollations(window.orderKeys, update, arg);
    List<RexNode> clonedPartitionKeys =
        visitList(window.partitionKeys, update, arg);
    RexWindowBound lowerBound = window.getLowerBound();
    RexWindowBound upperBound = window.getUpperBound();
    if (update[0]
        || (lowerBound != window.getLowerBound() && lowerBound != null)
        || (upperBound != window.getUpperBound() && upperBound != null)) {
      return new RexWindow(
          clonedPartitionKeys,
          clonedOrderKeys,
          lowerBound,
          upperBound,
          window.isRows());
    } else {
      return window;
    }
  }

  public RexNode visitSubQuery(RexSubQuery subQuery, P arg) {
    boolean[] update = {false};
    List<RexNode> clonedOperands = visitList(subQuery.operands, update, arg);
    if (update[0]) {
      return subQuery.clone(subQuery.getType(), clonedOperands);
    } else {
      return subQuery;
    }
  }

  public RexNode visitCall(final RexCall call, P arg) {
    boolean[] update = {false};
    List<RexNode> clonedOperands = visitList(call.operands, update, arg);
    if (update[0]) {
      // REVIEW jvs 8-Mar-2005:  This doesn't take into account
      // the fact that a rewrite may have changed the result type.
      // To do that, we would need to take a RexBuilder and
      // watch out for special operators like CAST and NEW where
      // the type is embedded in the original call.
      return new RexCall(
          call.getType(),
          call.getOperator(),
          clonedOperands);
    } else {
      return call;
    }
  }

  /**
   * Visits each of an array of expressions and returns an array of the
   * results.
   *
   * @param exprs  Array of expressions
   * @param update If not null, sets this to true if any of the expressions
   *               was modified
   * @return Array of visited expressions
   */
  protected RexNode[] visitArray(RexNode[] exprs, boolean[] update, P arg) {
    RexNode[] clonedOperands = new RexNode[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      RexNode operand = exprs[i];
      RexNode clonedOperand = operand.accept(this, arg);
      if ((clonedOperand != operand) && (update != null)) {
        update[0] = true;
      }
      clonedOperands[i] = clonedOperand;
    }
    return clonedOperands;
  }

  /**
   * Visits each of a list of expressions and returns a list of the
   * results.
   *
   * @param exprs  List of expressions
   * @param update If not null, sets this to true if any of the expressions
   *               was modified
   * @return Array of visited expressions
   */
  protected List<RexNode> visitList(
      List<? extends RexNode> exprs, boolean[] update, P arg) {
    ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
    for (RexNode operand : exprs) {
      RexNode clonedOperand = operand.accept(this, arg);
      if ((clonedOperand != operand) && (update != null)) {
        update[0] = true;
      }
      clonedOperands.add(clonedOperand);
    }
    return clonedOperands.build();
  }

  /**
   * Visits a list and writes the results to another list.
   */
  public void visitList(
      List<? extends RexNode> exprs, List<RexNode> outExprs, P arg) {
    for (RexNode expr : exprs) {
      outExprs.add(expr.accept(this, arg));
    }
  }

  /**
   * Visits each of a list of field collations and returns a list of the
   * results.
   *
   * @param collations List of field collations
   * @param update     If not null, sets this to true if any of the expressions
   *                   was modified
   * @return Array of visited field collations
   */
  protected List<RexFieldCollation> visitFieldCollations(
      List<RexFieldCollation> collations, boolean[] update, P arg) {
    ImmutableList.Builder<RexFieldCollation> clonedOperands =
        ImmutableList.builder();
    for (RexFieldCollation collation : collations) {
      RexNode clonedOperand = collation.left.accept(this, arg);
      if ((clonedOperand != collation.left) && (update != null)) {
        update[0] = true;
        collation =
            new RexFieldCollation(clonedOperand, collation.right);
      }
      clonedOperands.add(collation);
    }
    return clonedOperands.build();
  }

  public RexNode visitCorrelVariable(RexCorrelVariable variable, P arg) {
    return variable;
  }

  public RexNode visitFieldAccess(RexFieldAccess fieldAccess, P arg) {
    RexNode before = fieldAccess.getReferenceExpr();
    RexNode after = before.accept(this, arg);

    if (before == after) {
      return fieldAccess;
    } else {
      return new RexFieldAccess(
          after,
          fieldAccess.getField());
    }
  }

  public RexNode visitInputRef(RexInputRef inputRef, P arg) {
    return inputRef;
  }

  public RexNode visitLocalRef(RexLocalRef localRef, P arg) {
    return localRef;
  }

  public RexNode visitLiteral(RexLiteral literal, P arg) {
    return literal;
  }

  public RexNode visitDynamicParam(RexDynamicParam dynamicParam, P arg) {
    return dynamicParam;
  }

  public RexNode visitRangeRef(RexRangeRef rangeRef, P arg) {
    return rangeRef;
  }

  /**
   * Applies this shuttle to each expression in a list.
   *
   * @return whether any of the expressions changed
   */
  public final <T extends RexNode> boolean mutate(List<T> exprList, P arg) {
    int changeCount = 0;
    for (int i = 0; i < exprList.size(); i++) {
      T expr = exprList.get(i);
      T expr2 = (T) apply(expr, arg); // Avoid NPE if expr is null
      if (expr != expr2) {
        ++changeCount;
        exprList.set(i, expr2);
      }
    }
    return changeCount > 0;
  }

  /**
   * Applies this shuttle to each expression in a list and returns the
   * resulting list. Does not modify the initial list.
   */
  public final <T extends RexNode> List<T> apply(List<T> exprList, P arg) {
    if (exprList == null) {
      return null;
    }
    final List<T> list2 = new ArrayList<>(exprList);
    if (mutate(list2, arg)) {
      return list2;
    } else {
      return exprList;
    }
  }

  /**
   * Applies this shuttle to each expression in an iterable.
   */
  public final Iterable<RexNode> apply(Iterable<? extends RexNode> iterable, final P arg) {
    return Iterables.transform(iterable, new Function<RexNode, RexNode>() {
      public RexNode apply(@Nullable RexNode t) {
        return t == null ? null : t.accept(RexBiShuttle.this, arg);
      }
    });
  }

  /**
   * Applies this shuttle to an expression, or returns null if the expression
   * is null.
   */
  public final RexNode apply(RexNode expr, P arg) {
    return (expr == null) ? null : expr.accept(this, arg);
  }
}

// End RexShuttle.java

