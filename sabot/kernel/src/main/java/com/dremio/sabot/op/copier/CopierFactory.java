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

import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionManager;
import java.util.List;
import org.apache.arrow.vector.FieldVector;

/** Abstraction of a factory class for assigning copiers for a list of field vectors */
public interface CopierFactory {

  /**
   * Assigns 2 byte copiers for the pairs of vectors in the inputs and outputs list
   *
   * @param inputs Input List
   * @param outputs Output List
   * @param isTargetVectorZeroedOut true if the target vector is zeroed out before copy
   * @return List containing the assigned copiers
   */
  List<FieldBufferCopier> getTwoByteCopiers(
      List<FieldVector> inputs, List<FieldVector> outputs, boolean isTargetVectorZeroedOut);

  /**
   * Assigns 2 byte copiers for the pairs of vectors in the inputs and outputs list
   *
   * @param inputs Input List
   * @param outputs Output List
   * @return List containing the assigned copiers
   */
  default List<FieldBufferCopier> getTwoByteCopiers(
      List<FieldVector> inputs, List<FieldVector> outputs) {
    return getTwoByteCopiers(inputs, outputs, true);
  }

  /**
   * Assigns 4 byte copiers for the pairs of vectors in the inputs and outputs list
   *
   * @param inputs Input List
   * @param outputs Output List
   * @return List containing the assigned copiers
   */
  List<FieldBufferCopier> getFourByteCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs);

  /**
   * Assigns 6 byte copiers for the pairs of vectors in the inputs and outputs list
   *
   * @param inputs Input List
   * @param outputs Output List
   * @return List containing the assigned copiers
   */
  List<FieldBufferCopier> getSixByteCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs);

  /**
   * Assigns conditional 6 byte copiers for the pairs of vectors in the inputs and outputs list
   *
   * @param inputs Input List
   * @param outputs Output List
   * @return List containing the assigned copiers
   */
  List<FieldBufferCopier> getSixByteConditionalCopiers(
      List<FieldVector[]> inputs, List<FieldVector> outputs);

  /**
   * Loads and instantiates a concrete implementation of CopierFactory
   *
   * @param config SabotConfig
   * @param optionManager OptionManager
   * @return A concrete implementation of CopierFactory
   */
  static CopierFactory getInstance(SabotConfig config, OptionManager optionManager) {
    return config.getInstanceWithConstructorArgType(
        "dremio.copier.factory.class",
        CopierFactory.class,
        new FieldBufferCopierFactory(optionManager),
        OptionManager.class,
        optionManager);
  }
}
