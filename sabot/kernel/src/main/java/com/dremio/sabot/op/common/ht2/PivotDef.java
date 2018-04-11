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
package com.dremio.sabot.op.common.ht2;

import java.util.List;

import org.apache.arrow.vector.FieldVector;

import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldMode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class PivotDef {
  private final int blockWidth;
  private final int variableCount;
  private final int bitCount;
  private final ImmutableList<VectorPivotDef> vectorPivots;
  private final ImmutableList<VectorPivotDef> fixedPivots;
  private final ImmutableList<VectorPivotDef> bitPivots;
  private final ImmutableList<VectorPivotDef> nonBitFixedPivots;
  private final ImmutableList<VectorPivotDef> variablePivots;
  private final List<FieldVector> outputVectors;

  public PivotDef(
      int blockWidth,
      int variableCount,
      int bitCount,
      List<VectorPivotDef> fields) {
    super();
    this.blockWidth = blockWidth;
    this.variableCount = variableCount;
    this.bitCount = bitCount;
    this.vectorPivots = ImmutableList.copyOf(fields);

    this.fixedPivots = FluentIterable.from(vectorPivots).filter(new Predicate<VectorPivotDef>(){
      @Override
      public boolean apply(VectorPivotDef input) {
        return input.getType().mode != FieldMode.VARIABLE;
      }}).toList();

    this.bitPivots = FluentIterable.from(vectorPivots).filter(new Predicate<VectorPivotDef>(){
      @Override
      public boolean apply(VectorPivotDef input) {
        return input.getType().mode == FieldMode.BIT;
      }}).toList();

    this.nonBitFixedPivots = FluentIterable.from(vectorPivots).filter(new Predicate<VectorPivotDef>(){
      @Override
      public boolean apply(VectorPivotDef input) {
        return input.getType().mode == FieldMode.FIXED;
      }}).toList();

    this.variablePivots = FluentIterable.from(vectorPivots).filter(new Predicate<VectorPivotDef>(){
      @Override
      public boolean apply(VectorPivotDef input) {
        return input.getType().mode == FieldMode.VARIABLE;
      }}).toList();

    this.outputVectors = FluentIterable.from(vectorPivots).transform(new Function<VectorPivotDef, FieldVector>(){
      @Override
      public FieldVector apply(VectorPivotDef input) {
        return input.getOutgoingVector();
      }}).toList();
  }

  public ImmutableList<VectorPivotDef> getBitPivots() {
    return bitPivots;
  }

  public ImmutableList<VectorPivotDef> getNonBitFixedPivots() {
    return nonBitFixedPivots;
  }

  public int getBlockWidth() {
    return blockWidth;
  }

  public int getVariableCount() {
    return variableCount;
  }

  public int getBitCount() {
    return bitCount;
  }

  public List<VectorPivotDef> getVectorPivots() {
    return vectorPivots;
  }

  public List<VectorPivotDef> getVariablePivots(){
    return variablePivots;
  }

  public List<VectorPivotDef> getFixedPivots(){
    return fixedPivots;
  }

  public List<FieldVector> getOutputVectors(){
    return outputVectors;
  }

}
