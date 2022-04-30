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
package com.dremio.sabot.op.copier;

import java.util.List;

import org.apache.arrow.vector.FieldVector;

import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Factory class containing methods for assigning copiers for a list of field vectors
 */
public class FieldBufferCopierFactory implements CopierFactory {

  private final OptionManager optionManager;
  public FieldBufferCopierFactory(OptionManager optionManager) {
    this.optionManager = optionManager;
  }

  @Override
  public List<FieldBufferCopier> getTwoByteCopiers(List<FieldVector> inputs, List<FieldVector> outputs) {
    ImmutableList.Builder<FieldBufferCopier> copiers = ImmutableList.builder();
    Preconditions.checkArgument(inputs.size() == outputs.size(), "Input and output lists must be same size.");
    for (int i = 0; i < inputs.size(); i++) {
      final FieldVector input = inputs.get(i);
      final FieldVector output = outputs.get(i);
      FieldBufferCopier2Util.addValueCopier(input, output, copiers, optionManager);
    }
    return copiers.build();
  }

  @Override
  public List<FieldBufferCopier> getFourByteCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs){
    return getCopiers(inputs, outputs, (input, output, copiers) -> FieldBufferCopier4Util.addValueCopier(input, output, copiers));
  }

  @Override
  public List<FieldBufferCopier> getSixByteCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs) {
    return getCopiers(inputs, outputs, (input, output, copiers) -> FieldBufferCopier6Util.addValueCopier(input, output, copiers));
  }

  @Override
  public List<FieldBufferCopier> getSixByteConditionalCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs) {
    return getCopiers(inputs, outputs, (input, output, copiers) -> ConditionalFieldBufferCopier6Util.addValueCopier(input, output, copiers));
  }

  @FunctionalInterface
  private interface ValueCopierAdder {
    void addValueCopier(final FieldVector[] source, final FieldVector target, ImmutableList.Builder<FieldBufferCopier> copiers);
  }

  private List<FieldBufferCopier> getCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs, ValueCopierAdder valueCopierAdder) {
    ImmutableList.Builder<FieldBufferCopier> copiers = ImmutableList.builder();
    Preconditions.checkArgument(inputs.size() == outputs.size(), "Input and output lists must be same size.");
    for(int i = 0; i < inputs.size(); i++){
      final FieldVector[] input = inputs.get(i);
      final FieldVector output = outputs.get(i);
      valueCopierAdder.addValueCopier(input, output, copiers);
    }
    return copiers.build();
  }
}
