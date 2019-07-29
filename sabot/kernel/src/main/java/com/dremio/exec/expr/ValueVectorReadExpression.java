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
package com.dremio.exec.expr;

import java.util.Collections;
import java.util.Iterator;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.record.TypedFieldId;

public class ValueVectorReadExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorReadExpression.class);

  private final TypedFieldId fieldId;
  // was pushed in 3.0 have to retain this so that serde does not break.
  private final EvaluationType evaluationType;

  public ValueVectorReadExpression(TypedFieldId tfId){
    this.fieldId = tfId;
    this.evaluationType = new EvaluationType();
    evaluationType.addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  public boolean hasReadPath(){
    return fieldId.hasRemainder();
  }

  public PathSegment getReadPath(){
    return fieldId.getRemainder();
  }

  public TypedFieldId getTypedFieldId(){
    return fieldId;
  }

  public boolean isSuperReader(){
    return fieldId.isHyperReader();
  }

  @Override
  public CompleteType getCompleteType() {
    return fieldId.getFinalType();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public int getSelfCost() {
    return 0;  // TODO
  }

  @Override
  public int getCumulativeCost() {
    return 0; // TODO
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ValueVectorReadExpression other = (ValueVectorReadExpression) obj;
    if (fieldId == null) {
      if (other.fieldId != null) {
        return false;
      }
    } else if (!fieldId.equals(other.fieldId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "ValueVectorReadExpression [fieldId=" + fieldId + "]";
  }

}
