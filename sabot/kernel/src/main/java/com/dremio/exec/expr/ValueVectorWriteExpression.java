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
package com.dremio.exec.expr;

import java.util.Iterator;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.collect.Iterators;

public class ValueVectorWriteExpression implements LogicalExpression {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorWriteExpression.class);

  private final TypedFieldId fieldId;
  private final LogicalExpression child;
  private final boolean safe;
  private final EvaluationType evaluationType;

  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child){
    this(fieldId, child, false);
  }

  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child, boolean safe){
    this.fieldId = fieldId;
    this.child = child;
    this.safe = safe;
    this.evaluationType = new EvaluationType();
    addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  @Override
  public boolean isEvaluationTypeSupported(EvaluationType.ExecutionType executionType) {
    return evaluationType.isEvaluationTypeSupported(executionType);
  }

  @Override
  public void addEvaluationType(EvaluationType.ExecutionType executionType) {
    evaluationType.addEvaluationType(executionType);
  }

  @Override
  public EvaluationType getEvaluationType() {
    return evaluationType;
  }

  @Override
  public CompleteType getCompleteType() {
    return CompleteType.NULL;
  }

  public boolean isSafe() {
    return safe;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  public LogicalExpression getChild() {
    return child;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(child);
  }

  @Override
  public int getSelfCost() {
    return 0;  // TODO
  }

  @Override
  public int getCumulativeCost() {
    return 0; // TODO
  }


}
