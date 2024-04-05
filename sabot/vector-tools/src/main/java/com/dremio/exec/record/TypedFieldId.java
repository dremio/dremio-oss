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
package com.dremio.exec.record;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.PathSegment;
import com.dremio.exec.vector.ObjectVector;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.arrow.vector.ValueVector;
import org.apache.commons.lang3.ArrayUtils;

public class TypedFieldId {
  private final CompleteType finalType;
  private final CompleteType secondaryFinal;
  private final CompleteType intermediateType;
  private final int[] fieldIds;
  private final boolean isHyperReader;
  private final boolean isListVector;
  private final boolean isListOrUnionInPath;
  private final PathSegment remainder;

  public TypedFieldId(CompleteType type, int... fieldIds) {
    this(type, type, type, false, null, false, fieldIds);
  }

  public TypedFieldId(CompleteType type, boolean isHyper, int... fieldIds) {
    this(type, type, type, isHyper, null, false, fieldIds);
  }

  public TypedFieldId(
      CompleteType intermediateType,
      CompleteType secondaryFinal,
      CompleteType finalType,
      boolean isHyper,
      PathSegment remainder,
      boolean isListInPath,
      int... fieldIds) {
    this(
        intermediateType,
        secondaryFinal,
        finalType,
        isHyper,
        false,
        remainder,
        isListInPath,
        fieldIds);
  }

  public TypedFieldId(
      CompleteType intermediateType,
      CompleteType secondaryFinal,
      CompleteType finalType,
      boolean isHyper,
      boolean isListVector,
      PathSegment remainder,
      int... fieldIds) {
    this(
        intermediateType,
        secondaryFinal,
        finalType,
        isHyper,
        isListVector,
        remainder,
        false,
        fieldIds);
  }

  public TypedFieldId(
      CompleteType intermediateType,
      CompleteType secondaryFinal,
      CompleteType finalType,
      boolean isHyper,
      boolean isListVector,
      PathSegment remainder,
      boolean isListInPath,
      int... fieldIds) {
    super();
    this.intermediateType = intermediateType;
    this.finalType = finalType;
    this.secondaryFinal = secondaryFinal;
    this.fieldIds = fieldIds;
    this.isHyperReader = isHyper;
    this.isListVector = isListVector;
    this.remainder = remainder;
    this.isListOrUnionInPath = isListInPath;
  }

  public TypedFieldId cloneWithChild(int id) {
    int[] fieldIds = ArrayUtils.add(this.fieldIds, id);
    return new TypedFieldId(
        intermediateType,
        secondaryFinal,
        finalType,
        isHyperReader,
        remainder,
        isListOrUnionInPath,
        fieldIds);
  }

  public PathSegment getLastSegment() {
    if (remainder == null) {
      return null;
    }
    PathSegment seg = remainder;
    while (seg.getChild() != null) {
      seg = seg.getChild();
    }
    return seg;
  }

  public TypedFieldId cloneWithRemainder(PathSegment remainder) {
    return new TypedFieldId(
        intermediateType,
        secondaryFinal,
        finalType,
        isHyperReader,
        remainder,
        isListOrUnionInPath,
        fieldIds);
  }

  public boolean hasRemainder() {
    return remainder != null;
  }

  public PathSegment getRemainder() {
    return remainder;
  }

  public boolean isHyperReader() {
    return isHyperReader;
  }

  public boolean isListVector() {
    return isListVector;
  }

  public boolean isListOrUnionInPath() {
    return isListOrUnionInPath;
  }

  public CompleteType getIntermediateType() {
    return intermediateType;
  }

  public Class<? extends ValueVector> getIntermediateClass() {
    if (intermediateType == CompleteType.OBJECT) {
      return ObjectVector.class;
    }
    return intermediateType.getValueVectorClass();
  }

  public CompleteType getFinalType() {
    return finalType;
  }

  public int[] getFieldIds() {
    return fieldIds;
  }

  public CompleteType getSecondaryFinal() {
    return secondaryFinal;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final ArrayList<Integer> ids = new ArrayList<>();
    private CompleteType finalType;
    private CompleteType intermediateType;
    private CompleteType secondaryFinal;
    private PathSegment remainder;
    private boolean hyperReader = false;
    private boolean withIndex = false;
    private boolean isListVector = false;
    private boolean isListOrUnionInPath = false;

    public Builder addId(int id) {
      ids.add(id);
      return this;
    }

    public Builder withIndex() {
      withIndex = true;
      return this;
    }

    public Builder remainder(PathSegment remainder) {
      this.remainder = remainder;
      return this;
    }

    public Builder hyper() {
      this.hyperReader = true;
      return this;
    }

    public Builder listVector() {
      this.isListVector = true;
      return this;
    }

    public Builder finalType(CompleteType finalType) {
      this.finalType = finalType;
      return this;
    }

    public Builder secondaryFinal(CompleteType secondaryFinal) {
      this.secondaryFinal = secondaryFinal;
      return this;
    }

    public Builder intermediateType(CompleteType intermediateType) {
      this.intermediateType = intermediateType;
      return this;
    }

    public Builder isListOrUnionInPath(boolean isList) {
      this.isListOrUnionInPath = isList;
      return this;
    }

    public TypedFieldId build() {
      Preconditions.checkNotNull(intermediateType);
      Preconditions.checkNotNull(finalType);

      if (intermediateType == null) {
        intermediateType = finalType;
      }
      if (secondaryFinal == null) {
        secondaryFinal = finalType;
      }

      CompleteType actualFinalType = finalType;
      // CompleteType secondaryFinal = finalType;

      // if this has an index, switch to required type for output
      // if(withIndex && intermediateType == finalType) actualFinalType =
      // finalType.toBuilder().setMode(DataMode.REQUIRED).build();

      // if this isn't a direct access, switch the final type to nullable as offsets may be null.
      // TODO: there is a bug here with some things.
      // if(intermediateType != finalType) actualFinalType =
      // finalType.toBuilder().setMode(DataMode.OPTIONAL).build();
      return new TypedFieldId(
          intermediateType,
          secondaryFinal,
          actualFinalType,
          hyperReader,
          isListVector,
          remainder,
          isListOrUnionInPath,
          ids.stream().mapToInt(i -> i).toArray());
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(fieldIds);
    result = prime * result + ((finalType == null) ? 0 : finalType.hashCode());
    result = prime * result + ((intermediateType == null) ? 0 : intermediateType.hashCode());
    result = prime * result + (isHyperReader ? 1231 : 1237);
    result = prime * result + ((remainder == null) ? 0 : remainder.hashCode());
    result = prime * result + ((secondaryFinal == null) ? 0 : secondaryFinal.hashCode());
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
    TypedFieldId other = (TypedFieldId) obj;
    if (!Arrays.equals(fieldIds, other.fieldIds)) {
      return false;
    }
    if (finalType == null) {
      if (other.finalType != null) {
        return false;
      }
    } else if (!finalType.equals(other.finalType)) {
      return false;
    }
    if (intermediateType == null) {
      if (other.intermediateType != null) {
        return false;
      }
    } else if (!intermediateType.equals(other.intermediateType)) {
      return false;
    }
    if (isHyperReader != other.isHyperReader) {
      return false;
    }
    if (remainder == null) {
      if (other.remainder != null) {
        return false;
      }
    } else if (!remainder.equals(other.remainder)) {
      return false;
    }
    if (secondaryFinal == null) {
      if (other.secondaryFinal != null) {
        return false;
      }
    } else if (!secondaryFinal.equals(other.secondaryFinal)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "TypedFieldId [fieldIds="
        + (fieldIds != null
            ? Arrays.toString(Arrays.copyOf(fieldIds, Math.min(fieldIds.length, maxLen)))
            : null)
        + ", remainder="
        + remainder
        + "]";
  }
}
