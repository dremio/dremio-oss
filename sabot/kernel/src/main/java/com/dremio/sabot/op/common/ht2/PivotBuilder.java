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
package com.dremio.sabot.op.common.ht2;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.FieldVector;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class PivotBuilder {

  private static final int BITS_TO_FOUR_BYTES = 5;
  private static final int FOUR_BYTES_TO_BYTES = 2;
  public static final int BIT_OFFSET_MASK = 31;

  public static PivotDef getBlockDefinition(FieldVectorPair... fieldVectors) {
    return getBlockDefinition(ImmutableList.copyOf(fieldVectors));
  }

  public static PivotDef getBlockDefinition(Iterable<FieldVectorPair> fieldVectors) {
    List<VectorPivotDef> defs = new ArrayList<>();
    int bitOffset = 0;
    int fixedOffset = 0;
    int variableOffset = 0;

    for(FieldVectorPair v : fieldVectors){
      CompleteType type = CompleteType.fromField(v.getIncoming().getField());
      switch(type.toMinorType()){
      case BIT:
        // Would be too complicated to deal with the case where the valid bit is in one 4byte word, and the value is in the next 4byte word
        // The validity and value will be updated once in pivotBit method
        if ((bitOffset % (1 << BITS_TO_FOUR_BYTES)) == ((1 << BITS_TO_FOUR_BYTES) - 1)) {
          bitOffset++;
        }
        defs.add(new VectorPivotDef(FieldType.BIT, (bitOffset >>> BITS_TO_FOUR_BYTES) << FOUR_BYTES_TO_BYTES, bitOffset & BIT_OFFSET_MASK, (bitOffset + 1) & BIT_OFFSET_MASK, v));
        bitOffset+= 2;
        break;

      // 8 byte
      case BIGINT:
      case DATE:
      case TIMESTAMP:
      case FLOAT8:
      case INTERVALDAY:
        defs.add(new VectorPivotDef(FieldType.EIGHT_BYTE, (bitOffset >>> BITS_TO_FOUR_BYTES) << FOUR_BYTES_TO_BYTES, bitOffset & BIT_OFFSET_MASK, fixedOffset, v));
        bitOffset++;
        fixedOffset += 8;
        break;

      // 4 byte
      case FLOAT4:
      case INTERVALYEAR:
      case TIME:
      case INT:
        defs.add(new VectorPivotDef(FieldType.FOUR_BYTE, (bitOffset >>> BITS_TO_FOUR_BYTES) << FOUR_BYTES_TO_BYTES, bitOffset & BIT_OFFSET_MASK, fixedOffset, v));
        bitOffset++;
        fixedOffset += 4;
        break;

      // 16 byte
      case DECIMAL:
        defs.add(new VectorPivotDef(FieldType.SIXTEEN_BYTE, (bitOffset >>> BITS_TO_FOUR_BYTES) << FOUR_BYTES_TO_BYTES, bitOffset & BIT_OFFSET_MASK, fixedOffset, v));
        bitOffset++;
        fixedOffset += 16;
        break;

      // variable
      case VARBINARY:
      case VARCHAR:
        defs.add(new VectorPivotDef(FieldType.VARIABLE, (bitOffset >>> BITS_TO_FOUR_BYTES) << FOUR_BYTES_TO_BYTES, bitOffset & BIT_OFFSET_MASK, variableOffset, v));
        bitOffset++;
        variableOffset++;
        break;

      default:
        throw new UnsupportedOperationException("Unable to build table for field: " + Describer.describe(v.getIncoming().getField()));

      }
    }

    // calculate rounded up bits width.
    final int allBitsWidthInBytes = ( (int) Math.ceil(bitOffset/32.0d)) * 4;

    // rewrite field defs to a new list that adds the varlength, allBitsWidth, and VARLENGTH_ADDRESS in bytes to the fixed offset to get the block offset.
    final int nullShift = 0;
    final int valueShift = allBitsWidthInBytes;
    final ImmutableList<VectorPivotDef> shiftedDefs = FluentIterable.from(defs).transform(new Function<VectorPivotDef, VectorPivotDef>(){
      @Override
      public VectorPivotDef apply(VectorPivotDef input) {
        if(input.getType().mode == FieldMode.BIT || input.getType().mode == FieldMode.VARIABLE){
          // don't shift the value offset as it is fixed byte related.
          return input.cloneWithShift(nullShift, 0);
        }

        // shift the bit value and the null offset.
        return input.cloneWithShift(nullShift, valueShift);
      }}).toList();


    // Fixed data is four bytes (varlength) + allBitsWidth + fixed value width
    final int blockWidth;
    if(variableOffset > 0){
      blockWidth = allBitsWidthInBytes + fixedOffset + LBlockHashTable.VAR_OFFSET_SIZE;
    } else {
      blockWidth = allBitsWidthInBytes + fixedOffset;
    }

    return new PivotDef(blockWidth, variableOffset, bitOffset, shiftedDefs);
  }

  public static PivotInfo getBlockInfo(Iterable<FieldVector> fieldVectors) {
    int bitOffset = 0;
    int fixedOffset = 0;
    int variableOffset = 0;

    for(FieldVector v : fieldVectors){
      if (v == null) {
        // count1
        continue;
      }
      CompleteType type = CompleteType.fromField(v.getField());
      switch(type.toMinorType()){
        case BIT:
          if ((bitOffset % (1 << BITS_TO_FOUR_BYTES)) == ((1 << BITS_TO_FOUR_BYTES) - 1)) {
            bitOffset++;
          }
          bitOffset+= 2;
          break;

        /* 8 byte */
        case BIGINT:
        case DATE:
        case TIMESTAMP:
        case FLOAT8:
        case INTERVALDAY:
          bitOffset++;
          fixedOffset += 8;
          break;

        /* 4 byte */
        case FLOAT4:
        case INTERVALYEAR:
        case TIME:
        case INT:
          bitOffset++;
          fixedOffset += 4;
          break;

        /* 16 byte */
        case DECIMAL:
          bitOffset++;
          fixedOffset += 16;
          break;

        /* variable */
        case VARBINARY:
        case VARCHAR:
          bitOffset++;
          variableOffset++;
          break;

        default:
          throw new UnsupportedOperationException("Unable to build table for field: " + Describer.describe(v.getField()));
      }
    }

    // calculate rounded up bits width.
    final int allBitsWidthInBytes = ( (int) Math.ceil(bitOffset/32.0d)) * 4;

    // Fixed data is four bytes (varlength) + allBitsWidth + fixed value width
    final int blockWidth;
    if(variableOffset > 0){
      blockWidth = allBitsWidthInBytes + fixedOffset + LBlockHashTable.VAR_OFFSET_SIZE;
    } else {
      blockWidth = allBitsWidthInBytes + fixedOffset;
    }

    return new PivotInfo(blockWidth, variableOffset);
  }

  public static class PivotInfo {
    private final int blockWidth;
    private final int numVarColumns;

    public PivotInfo(final int blockWidth, final int numVarColumns) {
      this.blockWidth = blockWidth;
      this.numVarColumns = numVarColumns;
    }

    public int getBlockWidth() {
      return blockWidth;
    }

    public int getNumVarColumns() {
      return numVarColumns;
    }
  }

  public static enum FieldType {
    BIT(FieldMode.BIT, -1),
    FOUR_BYTE(FieldMode.FIXED, 4),
    EIGHT_BYTE(FieldMode.FIXED,8),
    SIXTEEN_BYTE(FieldMode.FIXED, 16),
    VARIABLE(FieldMode.VARIABLE, -1);

    public final FieldMode mode;
    public final int byteSize; // Applicable only in FIXED mode. Will be -1 otherwise.

    FieldType(FieldMode mode, int byteSize){
      this.mode = mode;
      this.byteSize = byteSize;
    }

    public int getByteSize() {
      return byteSize;
    }

    public FieldMode getMode() {
      return mode;
    }
  }

  public static enum FieldMode {
    BIT, FIXED, VARIABLE;
  }

}
