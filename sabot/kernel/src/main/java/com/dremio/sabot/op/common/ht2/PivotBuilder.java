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

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class PivotBuilder {

  public static final int INDEX_WIDTH = 4;
  public static final int VAR_OFFSET = 4;
  public static final int VARLEN_SIZE = 4;
  private static final int BITS_TO_4BYTE_WORDS = 5;

  public static PivotDef getBlockDefinition(FieldVectorPair... fieldVectors) {
    return getBlockDefinition(FluentIterable.of(fieldVectors).toList());
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
        defs.add(new VectorPivotDef(FieldType.BIT, bitOffset >>> BITS_TO_4BYTE_WORDS, bitOffset, bitOffset + 1, v));
        bitOffset+= 2;
        break;

      // 8 byte
      case BIGINT:
      case DATE:
      case TIMESTAMP:
      case FLOAT8:
      case INTERVALDAY:
        defs.add(new VectorPivotDef(FieldType.EIGHT_BYTE, bitOffset >>> BITS_TO_4BYTE_WORDS, bitOffset, fixedOffset, v));
        bitOffset++;
        fixedOffset += 8;
        break;

      // 4 byte
      case FLOAT4:
      case INTERVALYEAR:
      case TIME:
      case INT:
        defs.add(new VectorPivotDef(FieldType.FOUR_BYTE, bitOffset >>> BITS_TO_4BYTE_WORDS, bitOffset, fixedOffset, v));
        bitOffset++;
        fixedOffset += 4;
        break;

      // 16 byte
      case DECIMAL:
        defs.add(new VectorPivotDef(FieldType.SIXTEEN_BYTE, bitOffset >>> BITS_TO_4BYTE_WORDS, bitOffset, fixedOffset, v));
        bitOffset++;
        fixedOffset += 16;
        break;

      // variable
      case VARBINARY:
      case VARCHAR:
        defs.add(new VectorPivotDef(FieldType.VARIABLE, bitOffset >>> BITS_TO_4BYTE_WORDS, bitOffset, variableOffset, v));
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

  public static enum FieldType {
    BIT(FieldMode.BIT),
    FOUR_BYTE(FieldMode.FIXED),
    EIGHT_BYTE(FieldMode.FIXED),
    SIXTEEN_BYTE(FieldMode.FIXED),
    VARIABLE(FieldMode.VARIABLE);

    public final FieldMode mode;
    FieldType(FieldMode mode){
      this.mode = mode;
    }
  }

  public static enum FieldMode {
    BIT, FIXED, VARIABLE;
  }

}
