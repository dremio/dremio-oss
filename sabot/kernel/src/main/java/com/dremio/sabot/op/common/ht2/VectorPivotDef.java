/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.vector.FieldVector;

import com.dremio.common.expression.Describer;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldType;
import com.google.common.annotations.VisibleForTesting;

/**
 * Class describing the way a particular vector should be pivoted.
 */
public class VectorPivotDef {
  private final FieldType type;
  private final int nullByteOffset;
  private final int nullBitOffset;

  // the address offset of the buffer in this buffer collection.
  // is bit offset if bit type, byte offset if fixed byte type, position offset in case of variable type.
  private final int offset;
  private final FieldVectorPair vector;

  @VisibleForTesting
  VectorPivotDef(FieldType type, int nullByteOffset, int nullBitOffset, int offset, FieldVector input, FieldVector output) {
    this(type, nullByteOffset, nullBitOffset, offset, new FieldVectorPair(input, output));
  }

  public VectorPivotDef(FieldType type, int nullByteOffset, int nullBitOffset, int offset, FieldVectorPair vector) {
    super();
    this.type = type;
    this.nullByteOffset = nullByteOffset;
    this.nullBitOffset = nullBitOffset;
    this.offset = offset;
    this.vector = vector;
  }

  public FieldType getType() {
    return type;
  }

  public int getNullByteOffset() {
    return nullByteOffset;
  }

  public int getNullBitOffset() {
    return nullBitOffset;
  }

  public int getOffset() {
    return offset;
  }

  public FieldVector getIncomingVector() {
    return vector.getIncoming();
  }

  public FieldVector getOutgoingVector() {
    return vector.getOutgoing();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + nullBitOffset;
    result = prime * result + nullByteOffset;
    result = prime * result + offset;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((vector == null) ? 0 : vector.hashCode());
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
    VectorPivotDef other = (VectorPivotDef) obj;
    if (nullBitOffset != other.nullBitOffset) {
      return false;
    }
    if (nullByteOffset != other.nullByteOffset) {
      return false;
    }
    if (offset != other.offset) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    if (vector == null) {
      if (other.vector != null) {
        return false;
      }
    } else if (!vector.equals(other.vector)) {
      return false;
    }
    return true;
  }

  public VectorPivotDef cloneWithShift(int nullShift, int valueShift){
    return new VectorPivotDef(type, nullShift + nullByteOffset, nullBitOffset, valueShift + offset, vector);
  }

  @Override
  public String toString() {
    return "VectorPivotDef [type=" + type + ", nullByteOffset=" + nullByteOffset + ", nullBitOffset=" + nullBitOffset
        + ", offset=" + offset + ", vector=" + Describer.describe(vector.getIncoming().getField()) + "]";
  }


}