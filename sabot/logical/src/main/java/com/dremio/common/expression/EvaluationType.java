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

package com.dremio.common.expression;

/**
 * Dummy class carried forward for backward compatibility purposes,
 * since in 3.0 expressions had this as one of the properties.
 */
public class EvaluationType {
  public enum CodeGenOption {
    // use Gandiva only to generate code
    // This is a test-hook to be used by test code
    GandivaOnly,
    // prefer Java to generate code
    Java,
    // prefer Gandiva to generate code
    Gandiva;

    public static final CodeGenOption DEFAULT = Java;

    // Converts optionName to an enum
    public static CodeGenOption getCodeGenOption(String optionName) {
      try {
        return CodeGenOption.valueOf(optionName);
      } catch (Exception e) {
        // The optionName may be the lower case string (e.g. java)
        for(CodeGenOption option : CodeGenOption.values()) {
          if (optionName.equalsIgnoreCase(option.toString())) {
            return option;
          }
        }
      }

      // unreachable code. The validator ensures that the value is one of the available choices
      return null;
    }
  }

  public enum ExecutionType {
    JAVA(0x1),
    GANDIVA(0x2);

    private final int executionType;

    ExecutionType(int executionType) {
      this.executionType = executionType;
    }

    private int getExecutionType() {
      return this.executionType;
    }
  }

  private int evaluationType;

  public EvaluationType() {
    this.evaluationType = 0;
  }

  /**
   * Checks if an evaluation type is supported
   * @param executionType Evaluation type to check
   * @return true if evaluation type is supported
   */
  public boolean isEvaluationTypeSupported(EvaluationType.ExecutionType executionType) {
    return ((evaluationType & executionType.getExecutionType()) != 0);
  }

  /**
   * Marks an evaluation type as supported
   * @param executionType Evaluation type to be added
   */
  public void addEvaluationType(EvaluationType.ExecutionType executionType) {
    evaluationType |= executionType.getExecutionType();
  }

  /**
   * Removes an evaluation type
   * @param executionType Evaluation type to be removed
   */
  public void removeEvaluationType(EvaluationType.ExecutionType executionType) {
    evaluationType &= ~ executionType.getExecutionType();
  }
}
