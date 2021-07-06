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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
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

public class ConstantExpressionIdentifier implements ExprVisitor<Boolean, ConstantExtractor, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConstantExpressionIdentifier.class);

  private static final ConstantExtractor DUMMY_EXTRACTOR = new ConstantExtractor() {};

  private ConstantExpressionIdentifier() {
  }

  /**
   * Get a list of expressions that mark boundaries into a constant space.
   *
   * @param e expression
   * @return a set of constant expression boundaries, may contain nested boundaries (e.g constant inside a constant)
   */
  public static ConstantExtractor getConstantExtractor(LogicalExpression e) {
    final DefaultConstantExtractor extractor = new DefaultConstantExtractor();
    ConstantExpressionIdentifier visitor = new ConstantExpressionIdentifier();

    if (e.accept(visitor, extractor) && extractor.isEmpty()) {
      // if we receive a constant value here but the map is empty, this means the entire tree is a constant.
      // note, we can't use a singleton collection here because we need an identity set.
      extractor.putConstantExpression(e);
      return extractor;
    } else if (extractor.isEmpty()) {
      // so we don't continue to carry around a map, we let it go here and simply return an empty set.
      return DUMMY_EXTRACTOR;
    } else {
      return extractor;
    }
  }

  public static boolean isExpressionConstant(LogicalExpression e) {
    return e.accept(new ConstantExpressionIdentifier(), null);
  }

  private boolean checkChildren(LogicalExpression e, DefaultConstantExtractor value,
                                boolean transmitsConstant) {
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
          // no need to scan entire tree, so short circuit as the caller is only interested to know if whole
          // expression is a constant
          break;
        }
      }
    }

    // if one or more clauses isn't constant, this isn't constant.  this also isn't a constant if it operates on a set.
    if ((!constant || !transmitsConstant) && value != null) {
      for (LogicalExpression c : constants) {
        value.putConstantExpression(c);
      }
    }
    return constant && transmitsConstant;
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, ConstantExtractor value) {
    throw new UnsupportedOperationException("FunctionCall is not expected here. " +
      "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, ConstantExtractor value)
    throws RuntimeException {
    return checkChildren(holder, (DefaultConstantExtractor) value,
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
  public Boolean visitIntConstant(ValueExpressions.IntExpression intExpr, ConstantExtractor value) {
    return true;
  }

  @Override
  public Boolean visitFloatConstant(ValueExpressions.FloatExpression fExpr, ConstantExtractor value) {
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
}
