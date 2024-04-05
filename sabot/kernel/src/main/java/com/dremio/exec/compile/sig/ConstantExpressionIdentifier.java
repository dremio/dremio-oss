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
package com.dremio.exec.compile.sig;

import com.dremio.common.expression.ArrayLiteralExpression;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.ListAggExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.Ordering;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DateExpression;
import com.dremio.common.expression.ValueExpressions.DecimalExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.IntervalDayExpression;
import com.dremio.common.expression.ValueExpressions.IntervalYearExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.ValueExpressions.TimeExpression;
import com.dremio.common.expression.ValueExpressions.TimeStampExpression;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.expr.fn.ComplexWriterFunctionHolder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConstantExpressionIdentifier
    implements ExprVisitor<Boolean, ConstantExtractor, RuntimeException> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ConstantExpressionIdentifier.class);

  private static final ConstantExtractor DUMMY_EXTRACTOR = new ConstantExtractor() {};

  private ConstantExpressionIdentifier() {}

  /**
   * Get a list of expressions that mark boundaries into a constant space.
   *
   * @param e expression
   * @return a set of constant expression boundaries, may contain nested boundaries (e.g constant
   *     inside a constant)
   */
  public static ConstantExtractor getConstantExtractor(LogicalExpression e) {
    final DefaultConstantExtractor extractor = new DefaultConstantExtractor();
    ConstantExpressionIdentifier visitor = new ConstantExpressionIdentifier();

    if (e.accept(visitor, extractor) && extractor.isEmpty()) {
      // if we receive a constant value here but the map is empty, this means the entire tree is a
      // constant.
      // note, we can't use a singleton collection here because we need an identity set.
      extractor.putConstantExpression(e);
      return extractor;
    } else if (extractor.isEmpty()) {
      // so we don't continue to carry around a map, we let it go here and simply return an empty
      // set.
      return DUMMY_EXTRACTOR;
    } else {
      return extractor;
    }
  }

  // returns a set of constants containing exps which are ValueExpressions only
  public static Set<LogicalExpression> getConstantExpressions(LogicalExpression e) {
    final Set<LogicalExpression> constantsSet = new HashSet<>();
    if (e instanceof ValueExpressions.ConstantExpression) {
      constantsSet.add(e);
      return constantsSet;
    }
    ConstantValueIdentifier constantValueIdentifier = new ConstantValueIdentifier();
    return e.accept(constantValueIdentifier, constantsSet);
  }

  public static boolean isExpressionConstant(LogicalExpression e) {
    return e.accept(new ConstantExpressionIdentifier(), null);
  }

  private boolean checkChildren(
      LogicalExpression e, DefaultConstantExtractor value, boolean transmitsConstant) {
    List<LogicalExpression> constants = new ArrayList<>();
    boolean constant = true;

    for (LogicalExpression child : e) {
      if (child.accept(this, value)) {
        if (value != null) {
          // fill only if we need to update all constant sub expressions
          constants.add(child);
        }
      } else {
        constant = false;
        if (value == null) {
          // no need to scan entire tree, so short circuit as the caller is only interested to know
          // if whole
          // expression is a constant
          break;
        }
      }
    }

    // if one or more clauses isn't constant, this isn't constant.  this also isn't a constant if it
    // operates on a set.
    if ((!constant || !transmitsConstant) && value != null) {
      for (LogicalExpression c : constants) {
        value.putConstantExpression(c);
      }
    }
    return constant && transmitsConstant;
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, ConstantExtractor value) {
    throw new UnsupportedOperationException(
        "FunctionCall is not expected here. "
            + "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Boolean visitFunctionHolderExpression(
      FunctionHolderExpression holder, ConstantExtractor value) throws RuntimeException {
    return checkChildren(
        holder,
        (DefaultConstantExtractor) value,
        !holder.isAggregating()
            && !holder.isRandom()
            && !(holder.getHolder() instanceof ComplexWriterFunctionHolder));
  }

  @Override
  public Boolean visitBooleanOperator(BooleanOperator op, ConstantExtractor value) {
    return checkChildren(op, (DefaultConstantExtractor) value, true);
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, ConstantExtractor value) {
    return checkChildren(ifExpr, (DefaultConstantExtractor) value, true);
  }

  @Override
  public Boolean visitCaseExpression(CaseExpression caseExpression, ConstantExtractor value) {
    return checkChildren(caseExpression, (DefaultConstantExtractor) value, true);
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, ConstantExtractor value) {
    return false;
  }

  @Override
  public Boolean visitInputReference(InputReference input, ConstantExtractor value) {
    return input.getReference().accept(this, value);
  }

  @Override
  public Boolean visitOrdering(Ordering e, ConstantExtractor value) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitListAggExpression(ListAggExpression e, ConstantExtractor value)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitArrayLiteralExpression(ArrayLiteralExpression e, ConstantExtractor value)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitIntConstant(ValueExpressions.IntExpression intExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitFloatConstant(
      ValueExpressions.FloatExpression fExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitLongConstant(LongExpression longExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitDateConstant(DateExpression dateExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitDecimalConstant(DecimalExpression decExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitTimeConstant(TimeExpression timeExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitIntervalYearConstant(IntervalYearExpression iyExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitIntervalDayConstant(IntervalDayExpression idExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitTimeStampConstant(TimeStampExpression tsExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression bExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString sExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, ConstantExtractor value)
      throws RuntimeException {
    return e.getInput().accept(this, value);
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, ConstantExtractor value) {
    return checkChildren(e, (DefaultConstantExtractor) value, false);
  }

  @Override
  public Boolean visitNullConstant(TypedNullConstant e, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitNullExpression(NullExpression e, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, ConstantExtractor value) {
    return e.getInput().accept(this, value);
  }

  private static final class DefaultConstantExtractor implements ConstantExtractor {
    private int totalConstants;
    private final Set<LogicalExpression> constantIdentitySet;
    private final Map<CompleteType, Integer> constantsPerType;

    private DefaultConstantExtractor() {
      this.totalConstants = 0;
      this.constantIdentitySet = Collections.newSetFromMap(new IdentityHashMap<>());
      this.constantsPerType = new HashMap<>();
    }

    @Override
    public boolean isLarge(int threshold) {
      return totalConstants > threshold;
    }

    @Override
    public Map<CompleteType, Integer> getDiscoveredConstants() {
      return constantsPerType;
    }

    @Override
    public Set<LogicalExpression> getConstantExpressionIdentitySet() {
      return constantIdentitySet;
    }

    private boolean isEmpty() {
      return constantIdentitySet.isEmpty();
    }

    private void putConstantExpression(LogicalExpression e) {
      constantIdentitySet.add(e);
      totalConstants++;
      constantsPerType.compute(e.getCompleteType(), (k, v) -> v == null ? 1 : ++v);
    }
  }

  static class ConstantValueIdentifier
      implements ExprVisitor<Set<LogicalExpression>, Set<LogicalExpression>, RuntimeException> {

    private Set<LogicalExpression> checkChildren(
        LogicalExpression expression, Set<LogicalExpression> constants) {
      for (LogicalExpression childExp : expression) {
        childExp.accept(this, constants);
      }
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitFunctionCall(FunctionCall call, Set<LogicalExpression> value)
        throws RuntimeException {
      throw new UnsupportedOperationException(
          "FunctionCall is not expected here. "
              + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public Set<LogicalExpression> visitFunctionHolderExpression(
        FunctionHolderExpression holder, Set<LogicalExpression> constants) throws RuntimeException {
      return checkChildren(holder, constants);
    }

    @Override
    public Set<LogicalExpression> visitIfExpression(
        IfExpression ifExpr, Set<LogicalExpression> constants) throws RuntimeException {
      return checkChildren(ifExpr, constants);
    }

    @Override
    public Set<LogicalExpression> visitCaseExpression(
        CaseExpression caseExpression, Set<LogicalExpression> constants) throws RuntimeException {
      return checkChildren(caseExpression, constants);
    }

    @Override
    public Set<LogicalExpression> visitBooleanOperator(
        BooleanOperator call, Set<LogicalExpression> constants) throws RuntimeException {
      return checkChildren(call, constants);
    }

    @Override
    public Set<LogicalExpression> visitSchemaPath(SchemaPath path, Set<LogicalExpression> constants)
        throws RuntimeException {
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitIntConstant(
        ValueExpressions.IntExpression intExpr, Set<LogicalExpression> constants)
        throws RuntimeException {
      constants.add(intExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitFloatConstant(
        ValueExpressions.FloatExpression fExpr, Set<LogicalExpression> constants)
        throws RuntimeException {
      constants.add(fExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitLongConstant(
        LongExpression longExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(longExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitDateConstant(
        DateExpression dateExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(dateExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitTimeConstant(
        TimeExpression timeExpression, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(timeExpression);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitTimeStampConstant(
        TimeStampExpression tsExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(tsExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitIntervalYearConstant(
        IntervalYearExpression iyExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(iyExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitIntervalDayConstant(
        IntervalDayExpression idExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(idExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitDecimalConstant(
        DecimalExpression decExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(decExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitDoubleConstant(
        DoubleExpression dExpr, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(dExpr);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitBooleanConstant(
        BooleanExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(e);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitQuotedStringConstant(
        QuotedString e, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(e);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitNullConstant(
        TypedNullConstant e, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(e);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitNullExpression(
        NullExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      constants.add(e);
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitUnknown(
        LogicalExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      return checkChildren(e, constants);
    }

    @Override
    public Set<LogicalExpression> visitCastExpression(
        CastExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      return e.getInput().accept(this, constants);
    }

    @Override
    public Set<LogicalExpression> visitConvertExpression(
        ConvertExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      return e.getInput().accept(this, constants);
    }

    @Override
    public Set<LogicalExpression> visitInputReference(
        InputReference e, Set<LogicalExpression> constants) throws RuntimeException {
      return e.getReference().accept(this, constants);
    }

    @Override
    public Set<LogicalExpression> visitOrdering(Ordering e, Set<LogicalExpression> constants)
        throws RuntimeException {
      return e.getField().accept(this, constants);
    }

    @Override
    public Set<LogicalExpression> visitListAggExpression(
        ListAggExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      for (LogicalExpression arg : e.args) {
        arg.accept(this, constants);
      }
      for (LogicalExpression ex : e.getOrderings()) {
        ex.accept(this, constants);
      }
      return constants;
    }

    @Override
    public Set<LogicalExpression> visitArrayLiteralExpression(
        ArrayLiteralExpression e, Set<LogicalExpression> constants) throws RuntimeException {
      for (LogicalExpression arg : e.getItems()) {
        arg.accept(this, constants);
      }

      return constants;
    }
  }
}
