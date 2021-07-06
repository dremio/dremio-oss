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
package com.dremio.sabot.op.llvm;

import java.util.Objects;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorWrapper;

/**
 * Refers to the fields referenced in LogicalExpression to be executed in Gandiva
 */
public class ReferencedField {
  private final FieldVector referencedFieldVector;
  private final TypedFieldId typedFieldId;
  private final Field newField;

  public ReferencedField(FieldVector referencedFieldVector, TypedFieldId typedFieldId) {
    this.typedFieldId = typedFieldId;
    this.referencedFieldVector = referencedFieldVector;
    this.newField = referencedFieldVector.getField();
  }

  public ReferencedField(FieldVector referencedFieldVector, TypedFieldId typedFieldId, VectorWrapper<?> vw) {
    this.typedFieldId = typedFieldId;
    this.referencedFieldVector = referencedFieldVector;
    this.newField = getFullyQualifiedFieldName(vw, typedFieldId.getFieldIds());
  }

  public Field getFullyQualifiedFieldName(VectorWrapper<?> vw, int[] ids) {
    if (ids.length == 1) {
      return vw.getField();
    }
    StringBuilder sb = new StringBuilder(vw.getField().getName());
    ValueVector vector = vw.getValueVector();
    for (int i = 1; i < ids.length; i++) {
      final AbstractStructVector mapLike = AbstractStructVector.class.cast(vector);
      if (mapLike == null) {
        return null;
      }
      vector = mapLike.getChildByOrdinal(ids[i]);
      sb.append(".").append(vector.getField().getName());
    }
    Field orig = referencedFieldVector.getField();
    String fieldName =  sb.toString();
    Field xformed = new Field(fieldName, orig.getFieldType(), orig.getChildren());
    return xformed;
  }

  public Field getModifiedField() {
    return newField;
  }

  public TypedFieldId getTypedFieldId() {
    return typedFieldId;
  }

  public FieldVector getReferencedFieldVector() {
    return referencedFieldVector;
  }

  public boolean isComplexType() {
    return typedFieldId.getFieldIds().length > 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReferencedField that = (ReferencedField) o;
    return referencedFieldVector.equals(that.referencedFieldVector) && typedFieldId.equals(that.typedFieldId) && newField.equals(that.newField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(referencedFieldVector, typedFieldId);
  }
}
